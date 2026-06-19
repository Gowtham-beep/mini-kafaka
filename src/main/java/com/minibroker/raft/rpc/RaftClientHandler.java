package com.minibroker.raft.rpc;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;

public class RaftClientHandler extends SimpleChannelInboundHandler<RaftMessage> {
    private final ConcurrentMap<Long, CompletableFuture<? extends RaftMessage>> pendingRequests;

    public RaftClientHandler(ConcurrentMap<Long, CompletableFuture<? extends RaftMessage>> pendingRequests) {
        this.pendingRequests = pendingRequests;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void channelRead0(ChannelHandlerContext ctx, RaftMessage msg) throws Exception {
        long correlationId = -1;
        
        if (msg instanceof AppendEntrieResponse) {
            correlationId = ((AppendEntrieResponse) msg).correlationId();
        } else if (msg instanceof RequestVoteResponse) {
            correlationId = ((RequestVoteResponse) msg).correlationId();
        } else {
            System.err.println("Received unexpected message type in Client pipeline: " + msg.getClass().getSimpleName());
            return;
        }

        CompletableFuture<RaftMessage> future = (CompletableFuture<RaftMessage>) pendingRequests.remove(correlationId);
        if (future != null) {
            future.complete(msg);
        } else {
            System.err.println("Received response for unknown or timed out request ID: " + correlationId);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        System.err.printf("Network pipeline exception caught on client channel %s: %s%n", 
            ctx.channel().remoteAddress(), cause.getMessage());
        ctx.close();
    }
}
