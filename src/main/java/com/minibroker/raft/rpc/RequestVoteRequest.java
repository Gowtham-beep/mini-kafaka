package com.minibroker.raft.rpc;

public record RequestVoteRequest(
    long term,
    String candidateId,
    long lastLogIndex,
    long lastLogTerm
){} 

