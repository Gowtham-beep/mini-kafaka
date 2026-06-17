package com.minibroker.raft.rpc;

import java.nio.channels.Channel;

import com.minibroker.raft.RaftNode;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

public class RaftServer {
    private final int port;
    private final RaftNode raftNode;

    private EventLoopGroup boassGroup;
    private EventLoopGroup workerGroup;

    private EventExecutorGroup rafEventExecutorGroup;
    private Channel serverChannel;

    public RaftServer(int port ,RaftNode raftNode){
        this.port = port;
        this.raftNode = raftNode;
    }

    public void start() throws Exception{
        boassGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
        rafEventExecutorGroup = new DefaultEventExecutorGroup();

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(boassGroup,workerGroup);
            .cha
    }
}
