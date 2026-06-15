package com.minibroker.raft;

import com.minibroker.log.SegmentedLog;
import com.minibroker.raft.rpc.RequestVoteRequest;
import com.minibroker.raft.rpc.RequestVoteResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RaftNodeLeaderElectionTest {

    private RaftNode raftNode;
    private SegmentedLog mockLog;
    private RpcClient mockRpcClient;
    private ElectionTimer mockElectionTimer;
    private RequestPurgatory mockPurgatory;

    @BeforeEach
    void setUp() {
        mockLog = mock(SegmentedLog.class);
        mockRpcClient = mock(RpcClient.class);
        mockElectionTimer = mock(ElectionTimer.class);
        mockPurgatory = mock(RequestPurgatory.class);

        when(mockLog.getLastOffset()).thenReturn(-1L);
    }

    private void initRaftNode(List<String> peers) {
        raftNode = new RaftNode(
                "node-1",
                mockLog,
                mockRpcClient,
                mockElectionTimer,
                mockPurgatory,
                peers
        );
    }

    private Object getPrivateField(String fieldName) throws Exception {
        Field field = RaftNode.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(raftNode);
    }

    @Test
    void testNodeTransitionsToCandidateAndIncrementsTermOnElectionTimeout() throws Exception {
        initRaftNode(List.of("node-2", "node-3"));
        when(mockRpcClient.sendrequestVote(anyString(), any())).thenReturn(new CompletableFuture<>());

        raftNode.handleElectionTimeout();

        // Use reflection to verify state and term
        assertEquals(RaftNode.NodeState.CANDIDATE, getPrivateField("state"));
        assertEquals(1L, getPrivateField("currentTerm"));

        // Verify request was broadcasted with new term
        ArgumentCaptor<RequestVoteRequest> requestCaptor = ArgumentCaptor.forClass(RequestVoteRequest.class);
        verify(mockRpcClient, times(2)).sendrequestVote(anyString(), requestCaptor.capture());
        assertEquals(1L, requestCaptor.getValue().term());
    }

    @Test
    void testNodeTransitionsToLeaderWhenItReceivesMajorityVotes() throws Exception {
        initRaftNode(List.of("node-2", "node-3")); // Total 3 nodes, quorum is 2

        CompletableFuture<RequestVoteResponse> peer2Future = new CompletableFuture<>();
        CompletableFuture<RequestVoteResponse> peer3Future = new CompletableFuture<>();

        when(mockRpcClient.sendrequestVote(eq("node-2"), any())).thenReturn(peer2Future);
        when(mockRpcClient.sendrequestVote(eq("node-3"), any())).thenReturn(peer3Future);

        raftNode.handleElectionTimeout();

        // Complete peer 2's future successfully (granting vote)
        peer2Future.complete(new RequestVoteResponse(1L, true));

        // State should now be LEADER since it got 1 vote + its own vote = 2 (majority)
        assertEquals(RaftNode.NodeState.LEADER, getPrivateField("state"));
        verify(mockElectionTimer, times(1)).stop(); // Leader stops election timer
    }

    @Test
    void testNodeRemainsCandidateIfItReceivesVotesButNotMajority() throws Exception {
        // Total 5 nodes, quorum is 3
        initRaftNode(List.of("node-2", "node-3", "node-4", "node-5"));

        CompletableFuture<RequestVoteResponse> peer2Future = new CompletableFuture<>();
        CompletableFuture<RequestVoteResponse> peer3Future = new CompletableFuture<>();
        CompletableFuture<RequestVoteResponse> peer4Future = new CompletableFuture<>();
        CompletableFuture<RequestVoteResponse> peer5Future = new CompletableFuture<>();

        when(mockRpcClient.sendrequestVote(eq("node-2"), any())).thenReturn(peer2Future);
        when(mockRpcClient.sendrequestVote(eq("node-3"), any())).thenReturn(peer3Future);
        when(mockRpcClient.sendrequestVote(eq("node-4"), any())).thenReturn(peer4Future);
        when(mockRpcClient.sendrequestVote(eq("node-5"), any())).thenReturn(peer5Future);

        raftNode.handleElectionTimeout();

        // Complete peer 2's future successfully (granting vote)
        peer2Future.complete(new RequestVoteResponse(1L, true));
        
        // Deny peer 3's vote
        peer3Future.complete(new RequestVoteResponse(1L, false));

        // Node got 1 vote + its own vote = 2. Needs 3 for majority.
        assertEquals(RaftNode.NodeState.CANDIDATE, getPrivateField("state"));
    }

    @Test
    void testNodeStepsDownFromCandidateToFollowerWhenItSeesHigherTerm() throws Exception {
        initRaftNode(List.of("node-2", "node-3"));

        CompletableFuture<RequestVoteResponse> peer2Future = new CompletableFuture<>();
        when(mockRpcClient.sendrequestVote(anyString(), any())).thenReturn(peer2Future);

        raftNode.handleElectionTimeout();

        // Complete peer 2's future with a higher term and vote denied
        peer2Future.complete(new RequestVoteResponse(2L, false));

        // State should step down to FOLLOWER and term should update to 2
        assertEquals(RaftNode.NodeState.FOLLOWER, getPrivateField("state"));
        assertEquals(2L, getPrivateField("currentTerm"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testLeaderInitializesNextIndexToLastLogIndexPlusOne() throws Exception {
        initRaftNode(List.of("node-2", "node-3"));
        when(mockLog.getLastOffset()).thenReturn(10L); // Set log offset to 10

        CompletableFuture<RequestVoteResponse> peer2Future = new CompletableFuture<>();
        when(mockRpcClient.sendrequestVote(anyString(), any())).thenReturn(peer2Future);

        raftNode.handleElectionTimeout();
        peer2Future.complete(new RequestVoteResponse(1L, true)); // Win election

        // Check nextIndex map
        Map<String, Long> nextIndex = (Map<String, Long>) getPrivateField("nextIndex");
        assertEquals(11L, nextIndex.get("node-2"));
        assertEquals(11L, nextIndex.get("node-3"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testLeaderInitializesMatchIndexToZero() throws Exception {
        initRaftNode(List.of("node-2", "node-3"));
        when(mockLog.getLastOffset()).thenReturn(10L);

        CompletableFuture<RequestVoteResponse> peer2Future = new CompletableFuture<>();
        when(mockRpcClient.sendrequestVote(anyString(), any())).thenReturn(peer2Future);

        raftNode.handleElectionTimeout();
        peer2Future.complete(new RequestVoteResponse(1L, true)); // Win election

        // Check matchIndex map
        Map<String, Long> matchIndex = (Map<String, Long>) getPrivateField("matchIndex");
        assertEquals(0L, matchIndex.get("node-2"));
        assertEquals(0L, matchIndex.get("node-3"));
    }
}
