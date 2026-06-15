package com.minibroker.raft;

import com.minibroker.log.SegmentedLog;
import com.minibroker.raft.rpc.RequestVoteRequest;
import com.minibroker.raft.rpc.RequestVoteResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class RaftNodeRequestVoteTest {

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
                List.of("node-2", "node-3")
        );
    }

    @Test
    void testFollowerGrantsVoteWhenCandidateHasHigherTermAndUpToDateLog() {
        // Arrange: Candidate has term 2, our term is 0. Candidate log is up-to-date.
        RequestVoteRequest request = new RequestVoteRequest(2, "candidate-A", 0, 0);

        // Act
        RequestVoteResponse response = raftNode.handleRequestVote(request);

        // Assert
        assertTrue(response.voteGranted());
        assertEquals(2, response.term());
        verify(mockElectionTimer, atLeast(1)).reset(); // Election timer must reset on vote, might reset twice if stepping down
    }

    @Test
    void testFollowerDeniesVoteWhenCandidateTermIsLowerThanCurrentTerm() {
        // Arrange: We force our node to have a higher term (e.g., term 2) by receiving a newer vote request first
        raftNode.handleRequestVote(new RequestVoteRequest(2, "candidate-A", 0, 0));

        // Candidate requests vote with term 1 (lower)
        RequestVoteRequest staleRequest = new RequestVoteRequest(1, "candidate-B", 0, 0);

        // Act
        RequestVoteResponse response = raftNode.handleRequestVote(staleRequest);

        // Assert
        assertFalse(response.voteGranted());
        assertEquals(2, response.term()); // Should return our higher term
    }

    @Test
    void testFollowerDeniesVoteWhenAlreadyVotedForDifferentCandidateInSameTerm() {
        // Arrange: Node votes for candidate-A in term 1
        raftNode.handleRequestVote(new RequestVoteRequest(1, "candidate-A", 0, 0));

        // Act: Candidate-B asks for vote in the same term 1
        RequestVoteRequest competingRequest = new RequestVoteRequest(1, "candidate-B", 0, 0);
        RequestVoteResponse response = raftNode.handleRequestVote(competingRequest);

        // Assert
        assertFalse(response.voteGranted());
    }

    @Test
    void testFollowerGrantsVoteToSameCandidateTwiceInSameTerm() {
        // Arrange: Node votes for candidate-A in term 1
        raftNode.handleRequestVote(new RequestVoteRequest(1, "candidate-A", 0, 0));

        // Act: Candidate-A asks for vote AGAIN in the same term 1 (e.g., duplicate RPC)
        RequestVoteRequest duplicateRequest = new RequestVoteRequest(1, "candidate-A", 0, 0);
        RequestVoteResponse response = raftNode.handleRequestVote(duplicateRequest);

        // Assert
        assertTrue(response.voteGranted());
        assertEquals(1, response.term());
    }

    @Test
    void testFollowerDeniesVoteWhenCandidateLogIsShorter() {
        // Arrange: Our node has a longer log (last offset = 5, term = 1)
        when(mockLog.getLastOffset()).thenReturn(5L);
        when(mockLog.getTermAtOffset(5L)).thenReturn(1L);

        // Candidate has a shorter log in the same term (last offset = 3, term = 1)
        RequestVoteRequest request = new RequestVoteRequest(2, "candidate-A", 3, 1);

        // Act
        RequestVoteResponse response = raftNode.handleRequestVote(request);

        // Assert
        assertFalse(response.voteGranted());
    }

    @Test
    void testFollowerDeniesVoteWhenCandidateLogIsInOlderTerm() {
        // Arrange: Our node's log ends at term 3 (offset 5)
        when(mockLog.getLastOffset()).thenReturn(5L);
        when(mockLog.getTermAtOffset(5L)).thenReturn(3L);

        // Candidate's log is very long, but ends at term 2 (older term wins over longer log)
        RequestVoteRequest request = new RequestVoteRequest(4, "candidate-A", 100, 2);

        // Act
        RequestVoteResponse response = raftNode.handleRequestVote(request);

        // Assert
        assertFalse(response.voteGranted());
    }

    @Test
    void testFollowerStepsDownAndGrantsVoteWhenCandidateTermIsHigher() throws Exception {
        // Arrange: Force our node to be a CANDIDATE or LEADER in term 1
        // We simulate this by having it handle an election timeout
        when(mockRpcClient.sendrequestVote(anyString(), any())).thenReturn(new CompletableFuture<>());
        raftNode.handleElectionTimeout(); // Now term=1, state=CANDIDATE, votedFor=node-1
        
        // Candidate-B comes along with term 2 and up-to-date log
        RequestVoteRequest request = new RequestVoteRequest(2, "candidate-B", 0, 0);

        // Act
        RequestVoteResponse response = raftNode.handleRequestVote(request);

        // Assert
        assertTrue(response.voteGranted()); // Vote granted because it steps down to follower
        assertEquals(2, response.term());
        verify(mockElectionTimer, atLeastOnce()).reset(); 
    }
}
