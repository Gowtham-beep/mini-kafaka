package com.minibroker.raft.rpc;

import java.util.concurrent.Executor;

import com.minibroker.raft.RaftNode;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import io.netty.util.concurrent.EventExecutorGroup;

public class RaftNodeHandler extends SimpleChannelInboundHandler<RaftMessage>{
    private final RaftNode raftNode;
    private final Executor stateMachineExecuter;

    public RaftNodeHandler(RaftNode raftNode, Executor stateMachineExecuter){
        this.raftNode = raftNode;
        this.stateMachineExecuter = stateMachineExecuter;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, RaftMessage msg){
        stateMachineExecuter.execute(() -> {
            try{
                switch(msg){
                case AppendEntriesRequest req ->{
                    AppendEntrieResponse response = raftNode.handleAppendEntriesRequest(req);
                    ctx.writeAndFlush(response).addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
                }
                case RequestVoteRequest req ->{
                    RequestVoteResponse response = raftNode.handleRequestVote(req);
                    ctx.writeAndFlush(response).addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
                }
                default->{
                    System.err.println("Received unexpected message type in Server pipeline: " + msg.getClass().getSimpleName());
                }
            }
            }catch(Exception e){
                ctx.fireExceptionCaught(e);
            }
        });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause){
        System.err.printf("Network pipeline exception caught on channel %s: %s%n", 
            ctx.channel().remoteAddress(), cause.getMessage());
        ctx.close();  
    }
}
