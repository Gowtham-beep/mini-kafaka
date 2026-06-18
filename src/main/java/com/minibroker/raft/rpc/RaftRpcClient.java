package com.minibroker.raft.rpc;


import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.minibroker.raft.RpcClient;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

public class RaftRpcClient implements RpcClient  {
    private final Bootstrap bootstrap;
    private final Map<String,Channel> peerChannels = new ConcurrentHashMap<>();
    private final Map<String,Channel> pendingrequests = new ConcurrentHashMap<>();
    private final AtomicLong correlationIdGenerator = new AtomicLong(0);

    public RaftRpcClient(EventLoopGroup workerGroup){
        this.bootstrap = new Bootstrap()
        .group(workerGroup)
        .channel(NioSocketChannel.class)
        .option(ChannelOption.TCP_NODELAY,true)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch){
               ChannelPipeline p = ch.pipeline();

               p.addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
               p.addLast(new RaftRpcDecoder());
               p.addLast(new ClientResponseHandler());

               p.addLast(new LengthFieldPrepender(4));
               p.addLast(new RaftRpcEncoder());
            }
        });
    }

    private CompletableFuture<Channel> getOrConnect(String peerId,String host,int port){
        Channel existingChannels = peerChannels.get(peerId);
        if(existingChannels!=null && !existingChannels.isActive()){
            return CompletableFuture.completedFuture(existingChannels);
        }
        CompletableFuture<Channel> futureChannel = new CompletableFuture<>();
        bootstrap.connect(host,port).addListener((ChannelFutureListener) future ->{
          if(future.isSuccess()){
            Channel channel = future.channel();
            peerChannels.put(host, existingChannels);

            channel.closeFuture().addListener(f->{
                peerChannels.remove(peerId);
            });
            futureChannel.complete(channel);
          }else{
            futureChannel.completeExceptionally(future.cause());
          }
        });
            return futureChannel;
    }

}

