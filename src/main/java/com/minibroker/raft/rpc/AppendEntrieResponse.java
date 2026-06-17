package com.minibroker.raft.rpc;

public record AppendEntrieResponse(
    long term,
    boolean success
) implements RaftMessage{}
