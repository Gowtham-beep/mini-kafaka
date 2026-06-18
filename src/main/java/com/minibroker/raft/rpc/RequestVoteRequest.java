package com.minibroker.raft.rpc;

public record RequestVoteRequest(
    long correlationId,
    long term,
    String candidateId,
    long lastLogIndex,
    long lastLogTerm
) implements RaftMessage{} 

