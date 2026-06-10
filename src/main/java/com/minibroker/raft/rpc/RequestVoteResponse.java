package com.minibroker.raft.rpc;

public record RequestVoteResponse(
    long term,
    boolean voteGranted
){}

