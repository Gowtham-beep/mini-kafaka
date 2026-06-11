package com.minibroker.raft.rpc;

public record RecordMetaData(
    long logicalOffset,
    long timeStamp
) {}
