package com.minibroker.raft.rpc;



import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

public class RaftRpcDecoder extends MessageToMessageDecoder<ByteBuf> {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
       
        if (in.readableBytes() < 9) {
            return;
        }
        byte opcode = in.readByte();
        long correlationId = in.readLong();
        switch (opcode) {
            case 1 -> out.add(decodeAppendEntriesRequest(in, correlationId));
            case 2 -> out.add(decodeAppendEntriesResponse(in, correlationId));
            case 3 -> out.add(decodeRequestVoteRequest(in, correlationId));
            case 4 -> out.add(decodeRequestVoteResponse(in, correlationId));
            default -> throw new IllegalArgumentException("Unknown wire opcode received: " + opcode);
        }
    }
    private AppendEntriesRequest decodeAppendEntriesRequest(ByteBuf in, long correlationId){
        long term = in.readLong();
        int leaderIdLen = in.readInt();
        String leaderId = in.readCharSequence(leaderIdLen, StandardCharsets.UTF_8).toString();  
        long prevLogIndex = in.readLong();
        long prevLogTerm = in.readLong();

        int enriesSize = in.readInt();
        List<LogEntry> entries = new ArrayList<>(enriesSize);
        for(int i=0;i<enriesSize;i++){
            long entryTerm = in.readLong();
            int payloadLen = in.readInt();

            byte[] payload = new byte[payloadLen];
            in.readBytes(payload);
            entries.add(new LogEntry(entryTerm,payload));
        }
        long leaderCommit = in.readLong();
        return new AppendEntriesRequest(correlationId,term,leaderId,prevLogIndex,prevLogTerm,entries,leaderCommit);
    }

    private AppendEntrieResponse decodeAppendEntriesResponse(ByteBuf in, long correlationId){
        long term = in.readLong();
        boolean success = in.readByte()==1;
        return new AppendEntrieResponse(correlationId,term,success);
    }

    private RequestVoteRequest decodeRequestVoteRequest(ByteBuf in, long correlationId){
        long term = in.readLong();
        int candidateIdLen = in.readInt();
        String candidateId = in.readCharSequence(candidateIdLen, StandardCharsets.UTF_8).toString();

        long lastLogIndex = in.readLong();
        long lastLogTerm = in.readLong();
        return new RequestVoteRequest(correlationId,term,candidateId,lastLogIndex,lastLogTerm);
    }

    private RequestVoteResponse decodeRequestVoteResponse(ByteBuf in, long correlationId){
        long term = in.readLong();
        boolean voteGranted = in.readByte()==1;
        return new RequestVoteResponse(correlationId,term,voteGranted);
    
    }
}