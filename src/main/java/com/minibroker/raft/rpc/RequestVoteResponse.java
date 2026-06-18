package com.minibroker.raft.rpc;

public record RequestVoteResponse(
    long correlationId,
    long term,
    boolean voteGranted
) implements RaftMessage{}

