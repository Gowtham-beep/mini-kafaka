package com.minibroker.raft;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.lang.reflect.Field;

import com.minibroker.log.SegmentedLog;
import com.minibroker.raft.rpc.RaftRpcClient;
import com.minibroker.raft.rpc.RaftServer;

public class RaftElectionIntegrationTest {

    private RaftNode node1, node2, node3;
    private RaftServer server1, server2, server3;
    private RaftRpcClient client1, client2, client3;
    private ProxyElectionTimer timer1, timer2, timer3;

    // Proxy timer to break the ElectionTimer <-> RaftNode circular dependency
    static class ProxyElectionTimer implements ElectionTimer {
        private RaftNode node;
        private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        private ScheduledFuture<?> currentTimer;

        public void setNode(RaftNode node) { this.node = node; }

        @Override
        public synchronized void reset() {
            if (currentTimer != null) currentTimer.cancel(false);
            long timeout = ThreadLocalRandom.current().nextLong(150, 300);
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
    public void setUp() throws Exception {
        Map<String, InetSocketAddress> peersFor1 = Map.of(
            "node-2", new InetSocketAddress("127.0.0.1", 9002),
            "node-3", new InetSocketAddress("127.0.0.1", 9003)
        );
        Map<String, InetSocketAddress> peersFor2 = Map.of(
            "node-1", new InetSocketAddress("127.0.0.1", 9001),
            "node-3", new InetSocketAddress("127.0.0.1", 9003)
        );
        Map<String, InetSocketAddress> peersFor3 = Map.of(
            "node-1", new InetSocketAddress("127.0.0.1", 9001),
            "node-2", new InetSocketAddress("127.0.0.1", 9002)
        );

        client1 = new RaftRpcClient(peersFor1);
        client2 = new RaftRpcClient(peersFor2);
        client3 = new RaftRpcClient(peersFor3);

        timer1 = new ProxyElectionTimer();
        timer2 = new ProxyElectionTimer();
        timer3 = new ProxyElectionTimer();

        SegmentedLog log1 = mock(SegmentedLog.class);
        SegmentedLog log2 = mock(SegmentedLog.class);
        SegmentedLog log3 = mock(SegmentedLog.class);
        when(log1.getLastOffset()).thenReturn(-1L);
        when(log2.getLastOffset()).thenReturn(-1L);
        when(log3.getLastOffset()).thenReturn(-1L);

        RequestPurgatory purg1 = mock(RequestPurgatory.class);
        RequestPurgatory purg2 = mock(RequestPurgatory.class);
        RequestPurgatory purg3 = mock(RequestPurgatory.class);

        node1 = new RaftNode("node-1", log1, client1, timer1, purg1, List.of("node-2", "node-3"));
        node2 = new RaftNode("node-2", log2, client2, timer2, purg2, List.of("node-1", "node-3"));
        node3 = new RaftNode("node-3", log3, client3, timer3, purg3, List.of("node-1", "node-2"));

        timer1.setNode(node1);
        timer2.setNode(node2);
        timer3.setNode(node3);

        server1 = new RaftServer(9001, node1);
        server2 = new RaftServer(9002, node2);
        server3 = new RaftServer(9003, node3);

        server1.start();
        server2.start();
        server3.start();

        timer1.reset();
        timer2.reset();
        timer3.reset();
    }

    @AfterEach
    public void tearDown() {
        timer1.shutDown();
        timer2.shutDown();
        timer3.shutDown();
        
        client1.shutDown();
        client2.shutDown();
        client3.shutDown();
        
        server1.shutDown();
        server2.shutDown();
        server3.shutDown();
    }

    private RaftNode.NodeState getNodeState(RaftNode node) throws Exception {
        Field stateField = RaftNode.class.getDeclaredField("state");
        stateField.setAccessible(true);
        return (RaftNode.NodeState) stateField.get(node);
    }

    private long getCurrentTerm(RaftNode node) throws Exception {
        Field termField = RaftNode.class.getDeclaredField("currentTerm");
        termField.setAccessible(true);
        return (long) termField.get(node);
    }

    @Test
    public void testElectionSettlesWithOneLeader() throws Exception {
        // Wait for election to settle. 
        // Timeouts are 150-300ms, so 2000ms is plenty for an election to finish.
        Thread.sleep(2000);

        RaftNode.NodeState state1 = getNodeState(node1);
        RaftNode.NodeState state2 = getNodeState(node2);
        RaftNode.NodeState state3 = getNodeState(node3);

        int leaderCount = 0;
        int followerCount = 0;

        if (state1 == RaftNode.NodeState.LEADER) leaderCount++;
        else if (state1 == RaftNode.NodeState.FOLLOWER) followerCount++;

        if (state2 == RaftNode.NodeState.LEADER) leaderCount++;
        else if (state2 == RaftNode.NodeState.FOLLOWER) followerCount++;

        if (state3 == RaftNode.NodeState.LEADER) leaderCount++;
        else if (state3 == RaftNode.NodeState.FOLLOWER) followerCount++;

        long term1 = getCurrentTerm(node1);
        long term2 = getCurrentTerm(node2);
        long term3 = getCurrentTerm(node3);

        assertEquals(1, leaderCount, "There should be exactly one LEADER.");
        assertEquals(2, followerCount, "There should be exactly two FOLLOWERs.");

        assertEquals(term1, term2, "Node 1 and Node 2 should have the same term.");
        assertEquals(term2, term3, "Node 2 and Node 3 should have the same term.");
        assertTrue(term1 > 0, "The settled term should be greater than 0.");
    }
}
