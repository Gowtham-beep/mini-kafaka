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
            case AppendEntriesRequest req -> encodeAppendEntriesRequest(req,out);
            case AppendEntrieResponse res -> encodeAppendEntrieResponse(res,out);
            case RequestVoteRequest req -> encodeRequestVoteRequest(req,out);
            case RequestVoteResponse res -> encodeRequestVoteResponse(res,out);
            default -> throw new IllegalArgumentException("Unknown message type: " + msg);
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

        if(req.entries()==null || req.entries().isEmpty()){
            out.writeInt(0);
        }else{
            out.writeInt(req.entries().size());
            for(LogEntry entry:req.entries()){
                out.writeLong(entry.term());
                out.writeInt(entry.payload().length);
                out.writeBytes(entry.payload());
            }
        }
        out.writeLong(req.leaderCommit());
    }

    private void encodeAppendEntrieResponse(AppendEntrieResponse res, ByteBuf out){
        out.writeByte(2);
        out.writeLong(res.term());
        out.writeByte(res.success()?1:0);
    }

    private void encodeRequestVoteRequest(RequestVoteRequest req, ByteBuf out){
        out.writeByte(3);
        out.writeLong(req.term());
        String candidateId = req.candidateId()!=null?req.candidateId():"";
        int candidateIdLen = ByteBufUtil.utf8Bytes(candidateId);
        out.writeInt(candidateIdLen);
        out.writeCharSequence(candidateId, StandardCharsets.UTF_8);
        out.writeLong(req.lastLogIndex());
        out.writeLong(req.lastLogTerm());
    }

    private void encodeRequestVoteResponse(RequestVoteResponse res, ByteBuf out) {
        out.writeByte(4);
        out.writeLong(res.term());
        out.writeByte(res.voteGranted() ? 1 : 0);
    }
}