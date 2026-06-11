package com.minibroker.raft;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import com.minibroker.log.Segment;
import com.minibroker.log.SegmentedLog;
import com.minibroker.raft.rpc.AppendEntriesRequest;
import com.minibroker.raft.rpc.AppendentrieResponse;
import com.minibroker.raft.rpc.LogEntry;
import com.minibroker.raft.rpc.RequestVoteRequest;
import com.minibroker.raft.rpc.RequestVoteResponse;

public class RaftNode {
    public enum NodeState{
        FOLLOWER,CANDIDATE,LEADER
    }
    private final ReentrantLock stateLock = new ReentrantLock();

    private final String myNodeId;
    private final String currentLeaderId = null;
    private final SegmentedLog log;

    private long currentTerm =0;
    private String votedFor = null;

    private final List<String> clusterPeers;

    private NodeState state = NodeState.FOLLOWER;
    private final long commitIndex = 0;
    private long lastApplied = 0;

    private final Map<String,Long> nextIndex = new ConcurrentHashMap<>();
    private final Map<String, Long> matchIndex = new ConcurrentHashMap<>(); 

    private final RpcClient rpcClient;
    private final ElectionTimer electionTimer;
    private final RequestPurgatory purgatory;

    public RaftNode(
        String myNodeId,
        SegmentedLog log,
        RpcClient rpcClient,
        ElectionTimer electionTimer,
        RequestPurgatory purgatory
        List<String> clusterPeers
    ){
        this.myNodeId = myNodeId;
        this.log = log;
        this.rpcClient = rpcClient;
        this.electionTimer = electionTimer;
        this.purgatory = purgatory;
        this.clusterPeers = clusterPeers;
    }

    public RequestVoteResponse handlerequestVote(RequestVoteRequest rpc){
        stateLock.lock();
        try{
            if(rpc.term()<currentTerm){
            return new RequestVoteResponse(currentTerm,false);
        }
        if(rpc.term()>currentTerm){
            stepDownToFollwer(rpc.term());
        }
        if(votedFor!=null && !votedFor.equals(rpc.candidateId())){
            return new RequestVoteResponse(currentTerm,false);
        }
        long myLastIndex = log.getLastOffset();
        long myLastTerm = (myLastIndex<0)?0:log.getTermAtOffset(myLastIndex);

        boolean candidateLogIsOlder = (rpc.lastLogTerm()<myLastTerm)||(rpc.lastLogTerm()==myLastTerm && rpc.lastLogIndex()<myLastIndex);

        if(candidateLogIsOlder){
        return new RequestVoteResponse(currentTerm, false);
        }

        votedFor = rpc.candidateId();
        electionTimer.reset();

        return new RequestVoteResponse(currentTerm,true);
        }finally{
            stateLock.unlock();
        }
    }

    public AppendentrieResponse handlAppendentrieRequest(AppendEntriesRequest rpc){
        stateLock.lock();
        try{
            if(rpc.term()<currentTerm){
                return new AppendentrieResponse(currentTerm,false);
            }
            if(rpc.term()>currentTerm || state!=NodeState.FOLLOWER){
                stepDownToFollwer(rpc.term());
            }
            electionTimer.reset();
            currentLeaderId = rpc.leaderId();

            long myLastIndex= log.getLastOffset();
            if(myLastIndex<rpc.prevLogIndex()){
                return new AppendentrieResponse(currentTerm,false);
            }
            
            if(rpc.prevLogIndex()>=0){
                long myPrevLogTerm = log.getTermAtOffset(rpc.prevLogIndex());
                if(myPrevLogTerm!=rpc.prevLogTerm()){
                    return new AppendentrieResponse(currentTerm,false);
                }
            }
            if(rpc.entries()!=null && !rpc.entries().isEmpty()){
                log.truncateFromOffset(rpc.prevLogIndex()+1);

                for(LogEntry entry: rpc.entries()){
                    log.append(entry.term(),entry.payload());
                }
            }
            if(rpc.leaderCommit()>commitIndex){
                commitIndex = Math.min(rpc.leaderCommit(),log.getLastOffset());
            }
            return new AppendentrieResponse(currentTerm,true);

            }finally{
            stateLock.unlock();
            }
        }
        private void stepDownToFollwer( long newTerm){
            stateLock.lock();
            try{
                stepDownToFollwoerLocked(newTerm);
            }finally{
                stateLock.unlock();
            }
        }

        private void stepDownToFollwoerLocked(long term){
            state = NodeState.FOLLOWER;
            currentTerm= term;
            votedFor=null;
            electionTimer.reset();
        }

    
        private void  becomeLeader(){
            stateLock.lock();
            try{
                state = NodeState.LEADER;
                electionTimer.stop();
                currentLeaderId=myNodeId;

                long nextIdx = log.getLastOffset();
                for(String peer:clusterPeers){
                    nextIndex.put(peer, nextIdx);
                    matchIndex.put(peer, 0L);
                }
                broadcastAppendEntries();

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
            int requiredQourm = (clusterPeers.size()/2)+1;
            RequestVoteRequest request = new RequestVoteRequest(campaignTerm,myNodeId,lastLogIndex,lastLogTerm);

            for(String peer: clusterPeers){
                networkClient.sendrequestVote(peer,request).thenAccept(response->{
                    stateLock.lock();
                    try{
                        if(currentTerm!=campaignTerm|| state!=NodeState.CANDIDATE){
                            return;
                        }
                        if(response.term()>currentTerm){
                            stepDownToFollwer(response.term());
                            return;
                        }
                        if(response.voteGranted()){
                            if(voteReceived.incrementAndGet()>=requiredQourm){
                                becomeLeader();
                            }
                        }
                    }finally{
                        stateLock.unlock();
                    }
                });
            }
            
        }
    
}
