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
import com.minibroker.Consumer;
import com.minibroker.Producer;
import com.minibroker.raft.rpc.RecordMetaData;

public class ConsumerFollowerCatchUpTest {
    private RaftNode node1, node2, node3;
    private RaftServer server1, server2, server3;
    private RaftRpcClient client1, client2, client3;
    private ProxyElectionTimer timer1, timer2, timer3;
    private SegmentedLog log1, log2, log3;
    private RequestPurgatory purg1, purg2, purg3;
    private ExecutorService consumerIoExecutor;

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
        // Use different ports to avoid colliding with other tests if run concurrently
        Map<String, InetSocketAddress> peersFor1 = Map.of(
            "node-2", new InetSocketAddress("127.0.0.1", 9021),
            "node-3", new InetSocketAddress("127.0.0.1", 9022)
        );
        Map<String, InetSocketAddress> peersFor2 = Map.of(
            "node-1", new InetSocketAddress("127.0.0.1", 9020),
            "node-3", new InetSocketAddress("127.0.0.1", 9022)
        );
        Map<String, InetSocketAddress> peersFor3 = Map.of(
            "node-1", new InetSocketAddress("127.0.0.1", 9020),
            "node-2", new InetSocketAddress("127.0.0.1", 9021)
        );

        client1 = new RaftRpcClient(peersFor1);
        client2 = new RaftRpcClient(peersFor2);
        client3 = new RaftRpcClient(peersFor3);

        timer1 = new ProxyElectionTimer();
        timer2 = new ProxyElectionTimer();
        timer3 = new ProxyElectionTimer();

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

        server1 = new RaftServer(9020, node1);
        server2 = new RaftServer(9021, node2);
        server3 = new RaftServer(9022, node3);

        server1.start();
        server2.start();
        server3.start();

        timer1.reset();
        timer2.reset();
        timer3.reset();

        consumerIoExecutor = Executors.newFixedThreadPool(2);
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

        if (consumerIoExecutor != null) consumerIoExecutor.shutdownNow();
    }

    private RaftNode.NodeState getNodeState(RaftNode node) throws Exception {
        Field stateField = RaftNode.class.getDeclaredField("state");
        stateField.setAccessible(true);
        return (RaftNode.NodeState) stateField.get(node);
    }

    @Test
    public void testFollowerCatchUpRead() throws Exception {
        // 1. Wait for election to settle
        Thread.sleep(2000);

        RaftNode leaderNode = null;
        RaftNode followerNode = null;
        
        if (getNodeState(node1) == RaftNode.NodeState.LEADER) {
            leaderNode = node1;
            followerNode = node2;
        } else if (getNodeState(node2) == RaftNode.NodeState.LEADER) {
            leaderNode = node2;
            followerNode = node3;
        } else if (getNodeState(node3) == RaftNode.NodeState.LEADER) {
            leaderNode = node3;
            followerNode = node1;
        }

        assertNotNull(leaderNode, "A leader should be elected");
        assertNotNull(followerNode, "A follower should exist");

        Producer producer = new Producer(leaderNode);
        Consumer consumer = new Consumer(followerNode, consumerIoExecutor);

        // 2. Write a message and wait for it to commit (so offset 0 is created and committed)
        byte[] payload0 = "msg0".getBytes();
        RecordMetaData meta0 = producer.send(payload0).get(2, TimeUnit.SECONDS);
        assertEquals(0, meta0.logicalOffset());

        // Wait a bit to ensure the follower has also committed it
        Thread.sleep(1000);

        // 3. Prove the fast path works
        List<byte[]> batchFast = consumer.read(0, 1, 1000).get(2, TimeUnit.SECONDS);
        assertEquals(1, batchFast.size());
        assertArrayEquals(payload0, batchFast.get(0));

        // 4. Prove the slow path works
        // Call consumer.read() for offset 1, which doesn't exist yet
        CompletableFuture<List<byte[]>> slowReadFuture = consumer.read(1, 1, 5000);
        
        // Assert it hasn't completed yet
        assertFalse(slowReadFuture.isDone(), "Future should be parked since offset 1 doesn't exist yet");

        // Call producer.send() to create it on the leader
        byte[] payload1 = "msg1".getBytes();
        RecordMetaData meta1 = producer.send(payload1).get(2, TimeUnit.SECONDS);
        assertEquals(1, meta1.logicalOffset());

        // Assert the consumer's future resolves once quorum is reached and replicated to the follower
        List<byte[]> batchSlow = slowReadFuture.get(2, TimeUnit.SECONDS);
        assertEquals(1, batchSlow.size());
        assertArrayEquals(payload1, batchSlow.get(0));
    }
}
