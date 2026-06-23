package com.minibroker.raft;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.lang.reflect.Field;

import com.minibroker.log.SegmentedLog;
import com.minibroker.raft.rpc.RaftRpcClient;
import com.minibroker.raft.rpc.RaftServer;

public class RaftWriteIntegrationTest {

    private RaftNode node1, node2, node3;
    private RaftServer server1, server2, server3;
    private RaftRpcClient client1, client2, client3;
    private ProxyElectionTimer timer1, timer2, timer3;
    private SegmentedLog log1, log2, log3;
    private RequestPurgatory purg1, purg2, purg3;

    // Proxy timer to break the ElectionTimer <-> RaftNode circular dependency
    static class ProxyElectionTimer implements ElectionTimer {
        private RaftNode node;
        private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        private ScheduledFuture<?> currentTimer;

        public void setNode(RaftNode node) { this.node = node; }

        @Override
        public synchronized void reset() {
            if (scheduler.isShutdown()) return;
            if (currentTimer != null) currentTimer.cancel(false);
            long timeout = ThreadLocalRandom.current().nextLong(150, 300);
            try {
                currentTimer = scheduler.schedule(() -> {
                    if (node != null) {
                        try {
                            node.handleElectionTimeout();
                        } catch (Exception e) {
                            e.printStackTrace();
                            reset();
                        }
                    }
                }, timeout, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException e) {
                // Ignore if scheduler shut down concurrently
            }
        }

        @Override
        public synchronized void stop() {
            if (currentTimer != null) currentTimer.cancel(false);
        }

        @Override
        public void shutDown() {
            scheduler.shutdownNow();
        }
    }

    @BeforeEach
    public void setUp(@TempDir Path rootTempDir) throws Exception {
        // Use different ports to avoid colliding with RaftElectionIntegrationTest if run concurrently
        Map<String, InetSocketAddress> peersFor1 = Map.of(
            "node-2", new InetSocketAddress("127.0.0.1", 9011),
            "node-3", new InetSocketAddress("127.0.0.1", 9012)
        );
        Map<String, InetSocketAddress> peersFor2 = Map.of(
            "node-1", new InetSocketAddress("127.0.0.1", 9010),
            "node-3", new InetSocketAddress("127.0.0.1", 9012)
        );
        Map<String, InetSocketAddress> peersFor3 = Map.of(
            "node-1", new InetSocketAddress("127.0.0.1", 9010),
            "node-2", new InetSocketAddress("127.0.0.1", 9011)
        );

        client1 = new RaftRpcClient(peersFor1);
        client2 = new RaftRpcClient(peersFor2);
        client3 = new RaftRpcClient(peersFor3);

        timer1 = new ProxyElectionTimer();
        timer2 = new ProxyElectionTimer();
        timer3 = new ProxyElectionTimer();

        // Step 1: Real SegmentedLogs backed by isolated disk directories
        log1 = new SegmentedLog(0, rootTempDir.resolve("node-1"));
        log2 = new SegmentedLog(0, rootTempDir.resolve("node-2"));
        log3 = new SegmentedLog(0, rootTempDir.resolve("node-3"));

        purg1 = new RequestPurgatory();
        purg2 = new RequestPurgatory();
        purg3 = new RequestPurgatory();

        node1 = new RaftNode("node-1", log1, client1, timer1, purg1, List.of("node-2", "node-3"));
        node2 = new RaftNode("node-2", log2, client2, timer2, purg2, List.of("node-1", "node-3"));
        node3 = new RaftNode("node-3", log3, client3, timer3, purg3, List.of("node-1", "node-2"));

        timer1.setNode(node1);
        timer2.setNode(node2);
        timer3.setNode(node3);

        server1 = new RaftServer(9010, node1);
        server2 = new RaftServer(9011, node2);
        server3 = new RaftServer(9012, node3);

        server1.start();
        server2.start();
        server3.start();

        timer1.reset();
        timer2.reset();
        timer3.reset();
    }

    @AfterEach
    public void tearDown() {
        if (timer1 != null) timer1.shutDown();
        if (timer2 != null) timer2.shutDown();
        if (timer3 != null) timer3.shutDown();
        
        if (client1 != null) client1.shutDown();
        if (client2 != null) client2.shutDown();
        if (client3 != null) client3.shutDown();
        
        if (server1 != null) server1.shutDown();
        if (server2 != null) server2.shutDown();
        if (server3 != null) server3.shutDown();

        if (node1 != null) node1.shutDown();
        if (node2 != null) node2.shutDown();
        if (node3 != null) node3.shutDown();

        if (log1 != null) log1.close();
        if (log2 != null) log2.close();
        if (log3 != null) log3.close();
    }

    private RaftNode.NodeState getNodeState(RaftNode node) throws Exception {
        Field stateField = RaftNode.class.getDeclaredField("state");
        stateField.setAccessible(true);
        return (RaftNode.NodeState) stateField.get(node);
    }
}
