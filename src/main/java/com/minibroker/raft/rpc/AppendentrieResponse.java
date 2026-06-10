package com.minibroker.raft.rpc;

public record AppendentrieResponse(
    long term,
    boolean success
){}
