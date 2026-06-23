package com.minibroker.raft;

import com.minibroker.log.SegmentedLog;
import com.minibroker.raft.rpc.AppendEntriesRequest;
import com.minibroker.raft.rpc.AppendEntrieResponse;
import com.minibroker.raft.rpc.LogEntry;
import com.minibroker.raft.rpc.RequestVoteRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RaftNodeAppendEntriesTest {

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

        raftNode = new RaftNode(
                "node-1",
                mockLog,
                mockRpcClient,
                mockElectionTimer,
                mockPurgatory,
                new FetchPurgatory(),
                List.of("node-2", "node-3")
        );
    }

    @Test
    void testNodeRejectsAppendEntriesFromStaleTermLeader() {
        // Arrange: Bump our node's term to 2
        raftNode.handleRequestVote(new RequestVoteRequest(1L, 2, "candidate-A", 0, 0));

        // Act: Stale leader from term 1 sends heartbeat
        AppendEntriesRequest request = new AppendEntriesRequest(1L, 1, "stale-leader", 0, 0, List.of(), 0);
        AppendEntrieResponse response = raftNode.handleAppendEntriesRequest(request);

        // Assert
        assertFalse(response.success());
        assertEquals(2, response.term()); // Should return our higher term
    }

    @Test
    void testNodeStepsDownToFollowerWhenAppendEntriesArrivesFromHigherTerm() {
        // Arrange: Node is candidate in term 1
        when(mockRpcClient.sendRequestVote(anyString(), any())).thenReturn(new CompletableFuture<>());
        raftNode.handleElectionTimeout();

        // Act: Leader from term 2 sends heartbeat
        AppendEntriesRequest request = new AppendEntriesRequest(1L, 2, "new-leader", -1, 0, List.of(), 0);
        AppendEntrieResponse response = raftNode.handleAppendEntriesRequest(request);

        // Assert
        assertTrue(response.success());
        assertEquals(2, response.term());
        verify(mockElectionTimer, atLeast(1)).reset(); // Timer should be reset
    }

    @Test
    void testNodeRejectsAppendEntriesWhenPrevLogIndexIsAheadOfLocalLog() {
        // Arrange: Local log ends at index 5
        when(mockLog.getLastOffset()).thenReturn(5L);

        // Act: Leader thinks we are at index 10
        AppendEntriesRequest request = new AppendEntriesRequest(1L, 1, "leader", 10, 1, List.of(), 0);
        AppendEntrieResponse response = raftNode.handleAppendEntriesRequest(request);

        // Assert
        assertFalse(response.success());
    }

    @Test
    void testNodeRejectsAppendEntriesWhenPrevLogTermDoesNotMatch() {
        // Arrange: Local log ends at index 5. At index 3, our term is 1.
        when(mockLog.getLastOffset()).thenReturn(5L);
        when(mockLog.getTermAtOffset(3L)).thenReturn(1L);

        // Act: Leader sends logs starting after index 3, but expects term 2 at index 3
        AppendEntriesRequest request = new AppendEntriesRequest(1L, 2, "leader", 3, 2, List.of(), 0);
        AppendEntrieResponse response = raftNode.handleAppendEntriesRequest(request);

        // Assert
        assertFalse(response.success());
    }

    @Test
    void testNodeAppendsEntriesCorrectlyWhenTimelineMatches() {
        // Arrange: Local log ends at index 5, term 1
        when(mockLog.getLastOffset()).thenReturn(5L);
        when(mockLog.getTermAtOffset(5L)).thenReturn(1L);

        byte[] payload = "data".getBytes();
        LogEntry newEntry = new LogEntry(2, payload);
        
        // Act: Leader sends new entry at index 6
        AppendEntriesRequest request = new AppendEntriesRequest(1L, 2, "leader", 5, 1, List.of(newEntry), 0);
        AppendEntrieResponse response = raftNode.handleAppendEntriesRequest(request);

        // Assert
        assertTrue(response.success());
        verify(mockLog, times(1)).append(2L, payload);
    }

    @Test
    void testNodeAdvancesCommitIndexWhenLeaderCommitIsHigher() throws Exception {
        // Arrange: Local log is caught up to index 10
        when(mockLog.getLastOffset()).thenReturn(10L);
        when(mockLog.getTermAtOffset(10L)).thenReturn(1L);

        // Act: Leader sends heartbeat with leaderCommit = 8
        AppendEntriesRequest request = new AppendEntriesRequest(1L, 1, "leader", 10, 1, List.of(), 8);
        raftNode.handleAppendEntriesRequest(request);

        // Assert: Read private commitIndex field via Reflection
        Field commitIndexField = RaftNode.class.getDeclaredField("commitIndex");
        commitIndexField.setAccessible(true);
        long currentCommitIndex = (long) commitIndexField.get(raftNode);

        assertEquals(8L, currentCommitIndex);
    }

    @Test
    void testNodeTruncatesConflictingEntriesBeforeAppendingNewOnes() {
        // Arrange: Local log goes up to index 5. At index 3, our local term is 1.
        when(mockLog.getLastOffset()).thenReturn(5L);
        when(mockLog.getTermAtOffset(2L)).thenReturn(1L); // Preceding log matches
        when(mockLog.getTermAtOffset(3L)).thenReturn(1L); // Conflicting term at index 3

        byte[] payload = "new-data".getBytes();
        LogEntry conflictingEntryReplacement = new LogEntry(2, payload);

        // Act: Leader sends new entry for index 3, but with term 2
        AppendEntriesRequest request = new AppendEntriesRequest(1L, 2, "leader", 2, 1, List.of(conflictingEntryReplacement), 0);
        AppendEntrieResponse response = raftNode.handleAppendEntriesRequest(request);

        // Assert
        assertTrue(response.success());
        verify(mockLog, times(1)).truncateFromOffset(3L); // Should truncate old data starting at index 3
        verify(mockLog, times(1)).append(2L, payload);    // Should append the new data
    }
}
