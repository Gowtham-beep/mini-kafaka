package com.minibroker.raft.rpc;

import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class RaftRpcEncoder extends MessageToByteEncoder<RaftMessage> {

    @Override
    protected void encode(ChannelHandlerContext ctx,RaftMessage msg,ByteBuf out) throws Exception{
        switch(msg){
            AppendEntriesRequest req -> encodeAppendEntriesRequest(req,out);
            AppendEntrieResponse res -> encodeAppendEntrieResponse(res,out);
            RequestVoteRequest req -> encodeRequestVoteRequest(req,out);
            RequestVoteResponse res -> encodeRequestVoteResponse(res,out);
        }
        
    }

    private void encodeAppendEntriesRequest(AppendEntriesRequest req, ByteBuf out){
        out.writeByte(1);
        out.writeLong(req.term());

        String leaderId = req.leaderId()!=null?req.leaderId():"";
        int leaderIdLen = ByteBufUtil.utf8Bytes(leaderId);
        out.writeInt(leaderIdLen);
        out.writeCharSequence(leaderId, StandardCharsets.UTF_8);

        out.writeLong(req.prevLogIndex());
        out.writeLong(req.prevLogTerm());

        if(req.entries()!=null && req.entries().isEmpty()){
            out.writeInt(0);
        }else{
            out.writeInt(req.entries().size());
            for(LogEntry entry:req.entries()){
                out.writeLong(entry.term());
                out.writeLong(entry.payload().length);
                out.writeBytes(entry.payload());
            }
        }
        out.writeLong(req.leaderCommit());

    }
}
