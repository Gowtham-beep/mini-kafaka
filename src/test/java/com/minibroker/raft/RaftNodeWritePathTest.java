package com.minibroker.raft;

import com.minibroker.log.SegmentedLog;
import com.minibroker.raft.rpc.AppendentrieResponse;
import com.minibroker.raft.rpc.RequestVoteResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RaftNodeWritePathTest {

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
        when(mockRpcClient.sendAppendEntries(anyString(), any())).thenReturn(new CompletableFuture<>());
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

    private void makeLeader() {
        when(mockRpcClient.sendrequestVote(anyString(), any())).thenReturn(new CompletableFuture<>());
        raftNode.handleElectionTimeout();

        // Complete the votes immediately (simulating we win the election)
        try {
            Field stateLockField = RaftNode.class.getDeclaredField("stateLock");
            stateLockField.setAccessible(true);
            
            // Simulating winning the election
            // For a cluster of 5 nodes, we need 2 more votes
            raftNode.handleRequestVote(new com.minibroker.raft.rpc.RequestVoteRequest(1L, "node-1", -1, 0)); // This won't do much but just in case
            
            // To properly mock the election win without complex async logic, we can use reflection 
            // but the cleanest way is just completing the futures if we intercept them
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private void makeLeaderProperly(int peerCount) {
        CompletableFuture<RequestVoteResponse> voteFuture = new CompletableFuture<>();
        when(mockRpcClient.sendrequestVote(anyString(), any())).thenReturn(voteFuture);
        raftNode.handleElectionTimeout();
        voteFuture.complete(new RequestVoteResponse(1L, true));
    }

    private Object getPrivateField(String fieldName) throws Exception {
        Field field = RaftNode.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(raftNode);
    }

    @Test
    void testAppendMessageThrowsNotLeaderExceptionWhenCalledOnFollower() {
        initRaftNode(List.of("node-2", "node-3"));
        
        CompletableFuture<?> future = raftNode.appendMessage("test".getBytes());

        assertTrue(future.isCompletedExceptionally());
        
        ExecutionException exception = assertThrows(ExecutionException.class, future::get);
        assertTrue(exception.getCause() instanceof NotLeaderException);
    }

    @Test
    void testAppendMessageRegistersInPurgatoryAndReturnsUnresolvedFuture() {
        initRaftNode(List.of("node-2", "node-3"));
        makeLeaderProperly(2); // Win election
        
        when(mockLog.append(anyLong(), any())).thenReturn(1L);

        CompletableFuture<?> future = raftNode.appendMessage("test".getBytes());

        verify(mockPurgatory, times(1)).put(eq(1L), any());
        assertFalse(future.isDone());
    }

    @Test
    void testOnFollowerAckDoesNotAdvanceCommitIndexUntilMajority() throws Exception {
        initRaftNode(List.of("node-2", "node-3", "node-4", "node-5")); // 5 nodes, quorum is 3
        makeLeaderProperly(4);
        
        // Log offset is at 1
        when(mockLog.getTermAtOffset(1L)).thenReturn(1L);

        // Receive 1 ack, total replicas = 2 (leader + 1 peer), which is less than 3
        raftNode.onFollowerAck("node-2", new AppendentrieResponse(1L, true), 1L);

        long commitIndex = (long) getPrivateField("commitIndex");
        assertEquals(0L, commitIndex);
    }

    @Test
    void testOnFollowerAckAdvancesCommitIndexAndResolvesFutureWhenMajority() throws Exception {
        initRaftNode(List.of("node-2", "node-3", "node-4", "node-5")); // 5 nodes, quorum is 3
        makeLeaderProperly(4);
        
        // Term matches so checkQuorum passes the safety rule
        when(mockLog.getTermAtOffset(1L)).thenReturn(1L);

        // 1st ack
        raftNode.onFollowerAck("node-2", new AppendentrieResponse(1L, true), 1L);
        assertEquals(0L, (long) getPrivateField("commitIndex"));

        // 2nd ack -> total replicas = 3 (majority)
        raftNode.onFollowerAck("node-3", new AppendentrieResponse(1L, true), 1L);

        long commitIndex = (long) getPrivateField("commitIndex");
        assertEquals(1L, commitIndex);
        
        verify(mockPurgatory, times(1)).resolveAllUpTo(1L);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testOnFollowerAckDecrementsNextIndexForPeerOnFailureResponse() throws Exception {
        initRaftNode(List.of("node-2", "node-3"));
        when(mockLog.getLastOffset()).thenReturn(10L); // So nextIndex becomes 11
        makeLeaderProperly(2);

        Map<String, Long> nextIndex = (Map<String, Long>) getPrivateField("nextIndex");
        assertEquals(11L, nextIndex.get("node-2"));

        when(mockRpcClient.sendAppendEntries(anyString(), any())).thenReturn(new CompletableFuture<>());

        // Node-2 rejects the log at index 10
        raftNode.onFollowerAck("node-2", new AppendentrieResponse(1L, false), 10L);

        // nextIndex should decrement by 1
        assertEquals(10L, nextIndex.get("node-2"));
        
        // Verify it retried sending entries
        verify(mockRpcClient, atLeastOnce()).sendAppendEntries(eq("node-2"), any());
    }
}
