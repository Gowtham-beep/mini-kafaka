package com.minibroker.raft;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.minibroker.log.SegmentedLog;
import com.minibroker.raft.rpc.AppendEntriesRequest;
import com.minibroker.raft.rpc.AppendEntrieResponse;
import com.minibroker.raft.rpc.LogEntry;
import com.minibroker.raft.rpc.RecordMetaData;
import com.minibroker.raft.rpc.RequestVoteRequest;
import com.minibroker.raft.rpc.RequestVoteResponse;

public class RaftNode {
    public enum NodeState{
        FOLLOWER,CANDIDATE,LEADER
    }
    private final ReentrantLock stateLock = new ReentrantLock();

    private final String myNodeId;
    private String currentLeaderId = null;
    private final SegmentedLog log;

    private long currentTerm = 0;
    private String votedFor = null;

    private final List<String> clusterPeers;

    private NodeState state = NodeState.FOLLOWER;
    private long commitIndex = -1;

    private final Map<String,Long> nextIndex = new ConcurrentHashMap<>();
    private final Map<String, Long> matchIndex = new ConcurrentHashMap<>(); 

    private final RpcClient rpcClient;
    private final ElectionTimer electionTimer;
    private final RequestPurgatory purgatory;
    private final FetchPurgatory fetchPurgatory;
    private final AtomicLong correlationIdGenerator = new AtomicLong(0);
    private final java.util.concurrent.ExecutorService raftConsensusExecutor;
    private final ScheduledExecutorService heartbeatScheduler;
    private ScheduledFuture<?> currentHeartbeatTask;
    private final long heartbeatIntervalMs = 50;

    public RaftNode(
        String myNodeId,
        SegmentedLog log,
        RpcClient rpcClient,
        ElectionTimer electionTimer,
        RequestPurgatory purgatory,
        FetchPurgatory fetchPurgatory,
        List<String> clusterPeers
    ){
        this.myNodeId = myNodeId;
        this.log = log;
        this.rpcClient = rpcClient;
        this.electionTimer = electionTimer;
        this.purgatory = purgatory;
        this.fetchPurgatory = fetchPurgatory;
        this.clusterPeers = clusterPeers;
        this.raftConsensusExecutor = java.util.concurrent.Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "raft-consensus-" + myNodeId);
            t.setDaemon(true);
            return t;
        });
        this.heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "raft-heartbeat-" + myNodeId);
            t.setDaemon(true);
            return t;
        });
    }

    public void shutDown() {
        if (currentHeartbeatTask != null) {
            currentHeartbeatTask.cancel(false);
            currentHeartbeatTask = null;
        }
        heartbeatScheduler.shutdownNow();
        raftConsensusExecutor.shutdownNow();
    }

    public RequestVoteResponse handleRequestVote(RequestVoteRequest rpc){
        stateLock.lock();
        try{
            if(rpc.term()<currentTerm){
            return new RequestVoteResponse(rpc.correlationId(), currentTerm,false);
        }
        if(rpc.term()>currentTerm){
            stepDownToFollower(rpc.term());
        }
        if(votedFor!=null && !votedFor.equals(rpc.candidateId())){
            return new RequestVoteResponse(rpc.correlationId(), currentTerm,false);
        }
        long myLastIndex = log.getLastOffset();
        long myLastTerm = (myLastIndex<0)?0:log.getTermAtOffset(myLastIndex);

        boolean candidateLogIsOlder = (rpc.lastLogTerm()<myLastTerm)||(rpc.lastLogTerm()==myLastTerm && rpc.lastLogIndex()<myLastIndex);

        if(candidateLogIsOlder){
        return new RequestVoteResponse(rpc.correlationId(), currentTerm, false);
        }

        votedFor = rpc.candidateId();
        electionTimer.reset();

        return new RequestVoteResponse(rpc.correlationId(), currentTerm,true);
        }finally{
            stateLock.unlock();
        }
    }

    public AppendEntrieResponse handleAppendEntriesRequest(AppendEntriesRequest rpc){
        stateLock.lock();
        try{
            if(rpc.term()<currentTerm){
                return new AppendEntrieResponse(rpc.correlationId(), currentTerm,false);
            }
            if(rpc.term()>currentTerm || state!=NodeState.FOLLOWER){
                stepDownToFollower(rpc.term());
            }
            electionTimer.reset();
            currentLeaderId = rpc.leaderId();

            long myLastIndex = log.getLastOffset();
            if(myLastIndex<rpc.prevLogIndex()){
                return new AppendEntrieResponse(rpc.correlationId(), currentTerm,false);
            }
            
            if(rpc.prevLogIndex()>=0){
                long myPrevLogTerm = log.getTermAtOffset(rpc.prevLogIndex());
                if(myPrevLogTerm!=rpc.prevLogTerm()){
                    return new AppendEntrieResponse(rpc.correlationId(), currentTerm,false);
                }
            }
            if(rpc.entries()!=null && !rpc.entries().isEmpty()){
                long index = rpc.prevLogIndex() + 1;
                boolean conflict = false;
                for (LogEntry entry : rpc.entries()) {
                    if (index <= log.getLastOffset()) {
                        if (log.getTermAtOffset(index) != entry.term()) {
                            log.truncateFromOffset(index);
                            conflict = true;
                        }
                    }
                    if (conflict || index > log.getLastOffset()) {
                        log.append(entry.term(), entry.payload());
                    }
                    index++;
                }
            }
            if(rpc.leaderCommit()>commitIndex){
                commitIndex = Math.min(rpc.leaderCommit(),log.getLastOffset());
                fetchPurgatory.wakeAllUpTo(commitIndex);
            }
            return new AppendEntrieResponse(rpc.correlationId(), currentTerm,true);

            }finally{
            stateLock.unlock();
            }
        }
        private void stepDownToFollower( long newTerm){
            stateLock.lock();
            try{
                stepDownToFollowerLocked(newTerm);
            }finally{
                stateLock.unlock();
            }
        }

        private void stepDownToFollowerLocked(long term){
            state = NodeState.FOLLOWER;
            currentTerm= term;
            votedFor=null;
            electionTimer.reset();
            
            if (currentHeartbeatTask != null) {
                currentHeartbeatTask.cancel(false);
                currentHeartbeatTask = null;
            }

            // Note: Since appendMessage() only inserts into purgatory while holding stateLock 
            // and state == LEADER, a non-leader will always have an empty purgatory. 
            // Therefore, unconditionally calling failAll() here is perfectly safe and a no-op for non-leaders.
            purgatory.failAll(new NotLeaderException(currentLeaderId));
        }

        private void  becomeLeader(){
            stateLock.lock();
            try{
                state = NodeState.LEADER;
                electionTimer.stop();
                currentLeaderId=myNodeId;

                long nextIdx = log.getLastOffset()+1;
                for(String peer:clusterPeers){
                    nextIndex.put(peer, nextIdx);
                    matchIndex.put(peer, -1L);
                }
                
                if (currentHeartbeatTask != null) {
                    currentHeartbeatTask.cancel(false);
                }
                currentHeartbeatTask = heartbeatScheduler.scheduleAtFixedRate(() -> {
                    try {
                        broadcastAppendEntries();
                    } catch (Exception e) {
                        System.err.println("Heartbeat broadcast failed: " + e.getMessage());
                    }
                }, 0, heartbeatIntervalMs, TimeUnit.MILLISECONDS);

            }finally{
                stateLock.unlock();
            }
        }
        
        public void handleElectionTimeout(){
            long campaignTerm;
            long lastLogIndex;
            long lastLogTerm;

            stateLock.lock();
            try{
                state = NodeState.CANDIDATE;
                currentTerm++;
                votedFor=myNodeId;
                electionTimer.reset();

                lastLogIndex = log.getLastOffset();
                lastLogTerm = (lastLogIndex<0)?0:log.getTermAtOffset(lastLogIndex);

                campaignTerm=currentTerm;
            }finally{
                stateLock.unlock();
            }


            AtomicInteger voteReceived = new AtomicInteger(1);
            int requiredQuorum = ((clusterPeers.size() + 1) / 2) + 1;
            RequestVoteRequest request = new RequestVoteRequest(correlationIdGenerator.incrementAndGet(), campaignTerm,myNodeId,lastLogIndex,lastLogTerm);

            if (voteReceived.get() >= requiredQuorum) {
                becomeLeader();
                return;
            }

            for(String peer: clusterPeers){
                rpcClient.sendRequestVote(peer,request).thenAcceptAsync(response->{
                    stateLock.lock();
                    try{
                        if(currentTerm!=campaignTerm|| state!=NodeState.CANDIDATE){
                            return;
                        }
                        if(response.term()>currentTerm){
                            stepDownToFollower(response.term());
                            return;
                        }
                        if(response.voteGranted()){
                            if(voteReceived.incrementAndGet()>=requiredQuorum){
                                becomeLeader();
                            }
                        }
                    }finally{
                        stateLock.unlock();
                    }
                }, raftConsensusExecutor);
            }
            
        }

        public CompletableFuture<RecordMetaData> appendMessage(byte[] payload, long timeoutMs) {
            long logicalOffset;
            var future = new CompletableFuture<RecordMetaData>();
            stateLock.lock();
            try {
                if (state != NodeState.LEADER) {
                    future.completeExceptionally(new NotLeaderException(currentLeaderId));
                    return future;
                }
                logicalOffset = log.append(currentTerm, payload);
                purgatory.put(logicalOffset, future);
                broadcastAppendEntries();
            } finally {
                stateLock.unlock();
            }
            
            final long finalLogicalOffset = logicalOffset;
            return future.orTimeout(timeoutMs, TimeUnit.MILLISECONDS)
                         .whenComplete((res, ex) -> {
                             if (ex instanceof java.util.concurrent.TimeoutException) {
                                 purgatory.remove(finalLogicalOffset, future);
                             }
                         });
        }

        public void onFollowerAck(String peer,AppendEntrieResponse response,long sentOffset){
            stateLock.lock();
            try{
                if(state!=NodeState.LEADER){
                    return;
                }
                if(response.term()>currentTerm){
                    stepDownToFollower(response.term());
                    return;
                }
                if(response.success()){
                    matchIndex.put(peer, sentOffset);
                    nextIndex.put(peer,sentOffset+1);

                    if(checkQuorum(sentOffset)){
                        commitIndex=sentOffset;
                        purgatory.resolveAllUpTo(sentOffset);
                        fetchPurgatory.wakeAllUpTo(commitIndex);
                    }
                }else{
                    long currentNextIndex = nextIndex.getOrDefault(peer, 1L);
                    if(currentNextIndex>0){
                        nextIndex.put(peer, currentNextIndex -1);
                        sendAppendEntriesToPeer(peer);
                    }
                }
            }finally{
                stateLock.unlock();
            }
        }
        
        private boolean checkQuorum(long targetOffset){
            if(targetOffset<=commitIndex){
                return false;
            }
            if(targetOffset<0 || log.getTermAtOffset(targetOffset)!=currentTerm){
                return false;
            }
            int replicaCount = 1;
            for(String peer: clusterPeers){
                if(matchIndex.getOrDefault(peer, -1L)>=targetOffset){
                    replicaCount++;
                }
            }
            int majorityThreshold = ((clusterPeers.size() + 1) / 2) + 1;
            return replicaCount>=majorityThreshold;
        }

        private void sendAppendEntriesToPeer(String peer) {
            stateLock.lock();
            try {
                if (state != NodeState.LEADER) {
                    return;
                }
                long nextIdxForPeer = nextIndex.containsKey(peer) ? nextIndex.get(peer) : log.getLastOffset() + 1;
                long prevLogIndex = nextIdxForPeer - 1;
                long prevLogTerm = 0;
                if (prevLogIndex >= 0) {
                    prevLogTerm = log.getTermAtOffset(prevLogIndex);
                }

                List<LogEntry> entries = new ArrayList<>();
                long lastOffset = log.getLastOffset();
                for (long idx = nextIdxForPeer; idx <= lastOffset; idx++) {
                    try {
                        byte[] payload = log.read(idx);
                        long term = log.getTermAtOffset(idx);
                        entries.add(new LogEntry(term, payload));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }

                AppendEntriesRequest request = new AppendEntriesRequest(
                    correlationIdGenerator.incrementAndGet(),
                    currentTerm,
                    myNodeId,
                    prevLogIndex,
                    prevLogTerm,
                    entries,
                    commitIndex
                );

                rpcClient.sendAppendEntries(peer, request).thenAcceptAsync(response -> {
                    onFollowerAck(peer, response, prevLogIndex + entries.size());
                }, raftConsensusExecutor);

            } finally {
                stateLock.unlock();
            }
        }

        private void broadcastAppendEntries() {
            for (String peer : clusterPeers) {
                sendAppendEntriesToPeer(peer);
            }
        }

    public FetchPurgatory getFetchPurgatory() {
        return fetchPurgatory;
    }

    public SegmentedLog getLog() {
        return log;
    }

    public long getCommitIndex() {
        return commitIndex;
    }
}
