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

        FetchPurgatory fetchPurg1 = new FetchPurgatory();
        FetchPurgatory fetchPurg2 = new FetchPurgatory();
        FetchPurgatory fetchPurg3 = new FetchPurgatory();
        node1 = new RaftNode("node-1", log1, client1, timer1, purg1, fetchPurg1, List.of("node-2", "node-3"));
        node2 = new RaftNode("node-2", log2, client2, timer2, purg2, fetchPurg2, List.of("node-1", "node-3"));
        node3 = new RaftNode("node-3", log3, client3, timer3, purg3, fetchPurg3, List.of("node-1", "node-2"));

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

    @Test
    public void testEndToEndWriteAndReplication() throws Exception {
        // 1. Wait for election to settle
        Thread.sleep(2000);

        RaftNode leaderNode = null;
        SegmentedLog leaderLog = null;
        
        if (getNodeState(node1) == RaftNode.NodeState.LEADER) {
            leaderNode = node1;
            leaderLog = log1;
        } else if (getNodeState(node2) == RaftNode.NodeState.LEADER) {
            leaderNode = node2;
            leaderLog = log2;
        } else if (getNodeState(node3) == RaftNode.NodeState.LEADER) {
            leaderNode = node3;
            leaderLog = log3;
        }

        assertNotNull(leaderNode, "A leader should have been elected");

        // 2. Construct Producer manually pointing directly to the leader for this test
        com.minibroker.Producer producer = new com.minibroker.Producer(leaderNode);

        byte[] testPayload = "Hello Distributed Disk".getBytes();

        // 3. Send a message
        CompletableFuture<com.minibroker.raft.rpc.RecordMetaData> future = producer.send(testPayload);

        // 4. Wait for quorum replication (blocks until future is resolved)
        com.minibroker.raft.rpc.RecordMetaData metadata = future.get(5, TimeUnit.SECONDS);

        assertNotNull(metadata);
        assertTrue(metadata.logicalOffset() >= 0, "Offset should be a valid non-negative index");

        // 5. Allow followers a moment to flush to disk (leader already committed to resolve the future)
        Thread.sleep(500);

        // 6. Assert all 3 nodes' SegmentedLogs actually contain the message on disk
        byte[] readFrom1 = log1.read(metadata.logicalOffset());
        byte[] readFrom2 = log2.read(metadata.logicalOffset());
        byte[] readFrom3 = log3.read(metadata.logicalOffset());

        assertArrayEquals(testPayload, readFrom1, "Node 1 log should match payload");
        assertArrayEquals(testPayload, readFrom2, "Node 2 log should match payload");
        assertArrayEquals(testPayload, readFrom3, "Node 3 log should match payload");
    }
    @Test
    public void testQuorumLossTriggersTimeoutException() throws Exception {
        // Deferred Scenario 1: (Lose one follower, still have quorum). 
        // We skip this for now because it is just the happy path with a missing node,
        // and doesn't exercise the specific failure mechanisms we just built (timeout and failAll).

        // 1. Wait for election to settle
        Thread.sleep(2000);

        RaftNode leaderNode = null;
        RaftServer followerServer1 = null;
        RaftServer followerServer2 = null;
        ProxyElectionTimer followerTimer1 = null;
        ProxyElectionTimer followerTimer2 = null;
        
        if (getNodeState(node1) == RaftNode.NodeState.LEADER) {
            leaderNode = node1;
            followerServer1 = server2;
            followerServer2 = server3;
            followerTimer1 = timer2;
            followerTimer2 = timer3;
        } else if (getNodeState(node2) == RaftNode.NodeState.LEADER) {
            leaderNode = node2;
            followerServer1 = server1;
            followerServer2 = server3;
            followerTimer1 = timer1;
            followerTimer2 = timer3;
        } else if (getNodeState(node3) == RaftNode.NodeState.LEADER) {
            leaderNode = node3;
            followerServer1 = server1;
            followerServer2 = server2;
            followerTimer1 = timer1;
            followerTimer2 = timer2;
        }

        assertNotNull(leaderNode, "A leader should have been elected");

        // 2. Scenario 2: Quorum loss
        followerServer1.shutDown();
        followerServer2.shutDown();
        followerTimer1.shutDown();
        followerTimer2.shutDown();

        com.minibroker.Producer producer = new com.minibroker.Producer(leaderNode);
        
        // 3. Send with a short timeout (500ms)
        CompletableFuture<com.minibroker.raft.rpc.RecordMetaData> future = producer.send("Timeout Payload".getBytes(), 500);

        // 4. Assert it throws TimeoutException wrapped in ExecutionException
        ExecutionException exception = assertThrows(ExecutionException.class, future::get);
        assertTrue(exception.getCause() instanceof java.util.concurrent.TimeoutException);
    }

    @Test
    public void testLeaderFailureTriggersNotLeaderException() throws Exception {
        // 1. Wait for election to settle
        Thread.sleep(2000);

        RaftNode leaderNode = null;
        RaftServer followerServer1 = null;
        RaftServer followerServer2 = null;
        ProxyElectionTimer followerTimer1 = null;
        ProxyElectionTimer followerTimer2 = null;
        
        if (getNodeState(node1) == RaftNode.NodeState.LEADER) {
            leaderNode = node1;
            followerServer1 = server2;
            followerServer2 = server3;
            followerTimer1 = timer2;
            followerTimer2 = timer3;
        } else if (getNodeState(node2) == RaftNode.NodeState.LEADER) {
            leaderNode = node2;
            followerServer1 = server1;
            followerServer2 = server3;
            followerTimer1 = timer1;
            followerTimer2 = timer3;
        } else if (getNodeState(node3) == RaftNode.NodeState.LEADER) {
            leaderNode = node3;
            followerServer1 = server1;
            followerServer2 = server2;
            followerTimer1 = timer1;
            followerTimer2 = timer2;
        }

        assertNotNull(leaderNode, "A leader should have been elected");

        // 2. Stop followers so they can't ack the append and resolve it early
        followerServer1.shutDown();
        followerServer2.shutDown();
        followerTimer1.shutDown();
        followerTimer2.shutDown();

        com.minibroker.Producer producer = new com.minibroker.Producer(leaderNode);
        
        // 3. Send a message with a long timeout
        CompletableFuture<com.minibroker.raft.rpc.RecordMetaData> future = producer.send("StepDown Payload".getBytes(), 5000);

        // 4. Force step down by simulating a request with a much higher term
        leaderNode.handleAppendEntriesRequest(new com.minibroker.raft.rpc.AppendEntriesRequest(
            1L, 9999L, "some-other-node", -1L, 0L, List.of(), -1L
        ));

        // 5. Assert the future is failed explicitly by failAll() due to stepping down
        ExecutionException exception = assertThrows(ExecutionException.class, future::get);
        assertTrue(exception.getCause() instanceof NotLeaderException);
    }
}
