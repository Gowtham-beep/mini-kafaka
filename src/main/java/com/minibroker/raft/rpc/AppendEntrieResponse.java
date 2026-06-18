package com.minibroker.raft.rpc;

public record AppendEntrieResponse(
    long correlationId,
    long term,
    boolean success
) implements RaftMessage{}
