package com.minibroker.raft.rpc;


import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.minibroker.raft.RpcClient;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

public class RaftRpcClient implements RpcClient  {
    private final Map<String,InetSocketAddress> peerAddresses;
    private final ConcurrentMap<String, CompletableFuture<Channel>> peerchannels;
    private final ConcurrentMap<Long,CompletableFuture<? extends RaftMessage>> pendingRequests;
    private final AtomicLong correlationIdGenerator;

    private final EventLoopGroup group;
    private final Bootstrap bootstrap;

    private static final long NETWORK_TIMEOUT_MS = 2000;

    public RaftRpcClient(Map<String,InetSocketAddress> peerAddresses){
        this.peerAddresses=peerAddresses;
        this.peerchannels = new ConcurrentHashMap<>();
        this.pendingRequests = new ConcurrentHashMap<>();
        this.correlationIdGenerator = new AtomicLong(0);

        this.group = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        this.bootstrap = new Bootstrap();
        bootstrap.group(group)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY,true)
            .option(ChannelOption.SO_KEEPALIVE,true)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch){
                    ChannelPipeline pipeline = ch.pipeline();
                    pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
                    pipeline.addLast("encoder", new RaftRpcEncoder());
                    pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(
                        10 * 1024 * 1024, 0, 4, 0, 4));
                    pipeline.addLast("rpcDecoder", new RaftRpcDecoder());
                    pipeline.addLast("clientHandler", new RaftClientHandler(pendingRequests));
                }
            });
    }
    @Override
    public CompletableFuture<AppendEntrieResponse> sendAppendEntries(String peer,AppendEntriesRequest request){
        CompletableFuture<AppendEntrieResponse> responseFuture = new CompletableFuture<>();
        long cid = correlationIdGenerator.incrementAndGet();
        pendingRequests.put(cid, responseFuture);
        responseFuture.orTimeout(NETWORK_TIMEOUT_MS,TimeUnit.MILLISECONDS)
            .whenComplete((res,ex)->pendingRequests.remove(cid));

        
        getOrCreateChannel(peer).whenComplete((channel,connectEx)->{
            if(connectEx!=null){
                responseFuture.completeExceptionally(connectEx);
                return;
            }

            AppendEntriesRequest envelope = new AppendEntriesRequest(
                cid, request.term(), request.leaderId(), request.prevLogIndex(),
                request.prevLogTerm(), request.entries(), request.leaderCommit()
            );

            channel.writeAndFlush(envelope).addListener((ChannelFutureListener) fire -> {
                if (!fire.isSuccess()) {
                    responseFuture.completeExceptionally(fire.cause());
                }
            });
        });

        return responseFuture;
    }


    @Override
    public CompletableFuture<RequestVoteResponse> sendRequestVote(String peer,RequestVoteRequest request){
        CompletableFuture<RequestVoteResponse> responseFuture = new CompletableFuture<>();
        long cid = correlationIdGenerator.incrementAndGet();
        pendingRequests.put(cid, responseFuture);
        responseFuture.orTimeout(NETWORK_TIMEOUT_MS,TimeUnit.MILLISECONDS)
            .whenComplete((res,ex)-> pendingRequests.remove(cid));

        getOrCreateChannel(peer).whenComplete((channel,connectEx)->{
            if(connectEx!=null){
                responseFuture.completeExceptionally(connectEx);
                return;
            }

        RequestVoteRequest envelope = new RequestVoteRequest(
           cid, request.term(), request.candidateId(), request.lastLogIndex(), request.lastLogTerm()
        );

        channel.writeAndFlush(envelope).addListener((ChannelFutureListener) fire -> {
                if (!fire.isSuccess()) {
                    responseFuture.completeExceptionally(fire.cause());
                }
            });
        });

        return responseFuture;
    }

    private CompletableFuture<Channel> getOrCreateChannel(String peerId){
        InetSocketAddress address = peerAddresses.get(peerId);
        if (address == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Unknown peer: " + peerId));
        }
        return peerchannels.compute(peerId,(key,existingFuture)->{
            if(existingFuture!=null && !existingFuture.isDone()){
                return existingFuture;
            }
            if(existingFuture!=null && existingFuture.isDone() && !existingFuture.isCompletedExceptionally()){
                return existingFuture;
            }

            CompletableFuture<Channel> connectFuture = new CompletableFuture<>();
            bootstrap.connect(address).addListener((ChannelFutureListener) future->{
             if(future.isSuccess()){
                Channel ch = future.channel();

                ch.closeFuture().addListener(cf-> peerchannels.remove(peerId,connectFuture));
                connectFuture.complete(ch);
                }else{
                    connectFuture.completeExceptionally(future.cause());
                }
            });
            return connectFuture;
        });
    }

    public void shutDown(){
        System.out.println("Halting outbound Raft RPC Client loops...");
        pendingRequests.values().forEach(future->{
            future.completeExceptionally(new CancellationException("Client shut down initiated."));
        });
        pendingRequests.clear();
        peerchannels.values().forEach(future->{
            future.whenComplete((channel,ex)->{
              if(channel!=null && channel.isActive()){
                channel.close();
              }  
            });
        });
        group.shutdownGracefully();
    }
}



    

