package com.minibroker.raft;

public interface ElectionTimer {
    void reset();
    void stop();
    void shutDown();
}
