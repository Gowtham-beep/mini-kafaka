package com.minibroker.raft.rpc;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.minibroker.raft.RaftNode;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;


public class RaftServer {
    private final int port;
    private final RaftNode raftNode;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    private ExecutorService raftExecutorService;
    private Channel serverChannel;

    public RaftServer(int port ,RaftNode raftNode){
        this.port = port;
        this.raftNode = raftNode;
    }

    public void start() throws Exception{
        bossGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        workerGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        raftExecutorService = Executors.newFixedThreadPool(4, runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName("raft-state-worker-" + thread.getId());
            thread.setDaemon(true);
            return thread;
        });

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup,workerGroup)
            .channel(NioServerSocketChannel.class)
            .option(ChannelOption.SO_REUSEADDR,true)
            .childOption(ChannelOption.SO_KEEPALIVE,true)
            .childOption(ChannelOption.TCP_NODELAY,true)
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch){
                    ChannelPipeline pipeline = ch.pipeline();

                    pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
                    pipeline.addLast("encoder", new RaftRpcEncoder());
                    
                    pipeline.addLast("Framedecoder", new LengthFieldBasedFrameDecoder(
                        10 * 1024 * 1024, 0, 4, 0, 4));
                    pipeline.addLast("decoder", new RaftRpcDecoder());
                    pipeline.addLast("raftHandler", new RaftNodeHandler(raftNode, raftExecutorService));
                } 
            });
            ChannelFuture future = bootstrap.bind(port).sync();
            this.serverChannel = future.channel();
            System.out.printf("Raft Server Node successfully bound and listening on TCP port %d%n", port);
    }

    public void shutDown(){
        System.out.printf("Initiating graceful shutdown sequence for Raft Server on port %d...%n", port);
        try{
            if(serverChannel!=null){
                serverChannel.close().sync();
            }
        }catch(InterruptedException e){
            Thread.currentThread().interrupt();
        }finally{
            if (raftExecutorService != null) {
                raftExecutorService.shutdown();
                try {
                    if (!raftExecutorService.awaitTermination(5, TimeUnit.SECONDS)) {
                        raftExecutorService.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    raftExecutorService.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
            if(workerGroup!=null) workerGroup.shutdownGracefully();
            if(bossGroup!=null) bossGroup.shutdownGracefully();

            System.out.printf("Raft Server on port %d successfully halted.%n", port);
        }
    }
}
