package com.minibroker.raft.rpc;

import java.util.List;

public record AppendEntriesRequest(long term,
    String leaderId,
    long prevLogIndex,
    long prevLogTerm,
    List<LogEntry> entries,
    long leaderCommit
) implements RaftMessage {}
