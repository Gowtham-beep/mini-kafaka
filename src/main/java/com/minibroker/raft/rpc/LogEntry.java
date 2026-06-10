package com.minibroker.raft.rpc;

public record LogEntry(
    long term,
    byte[] payload
){}