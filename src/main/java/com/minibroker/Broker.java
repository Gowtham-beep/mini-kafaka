package com.minibroker;

import java.net.InetSocketAddress;

public interface Broker {
    /**
     * Starts the broker, binding network ports and initiating Raft election/recovery.
     */
    void start();

    /**
     * Gracefully shuts down the broker and closes all underlying resources.
     */
    void shutdown();

    /**
     * Returns the current Raft role of this broker (LEADER, FOLLOWER, CANDIDATE).
     */
    Role getRole();

    /**
     * Returns a point-in-time snapshot of the broker's health and throughput metrics.
     */
    BrokerMetrics getMetrics();

    /**
     * Returns the client-facing network address of the current cluster leader.
     * Used by orchestrators and the broker's own network layer to serve Metadata requests.
     */
    InetSocketAddress getLeaderAddress();
}
