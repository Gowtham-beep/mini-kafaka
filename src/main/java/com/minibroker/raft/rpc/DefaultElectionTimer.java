package com.minibroker.raft.rpc;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import com.minibroker.raft.ElectionTimer;
import com.minibroker.raft.RaftNode;

public class DefaultElectionTimer implements ElectionTimer {

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> currentTimer;
    private RaftNode raftNode;
    
    public DefaultElectionTimer() {
        // Node will be set later via setNode() to break circular dependency
    }
    
    public void setNode(RaftNode node) {
        this.raftNode = node;
    }
    @Override
    public synchronized void reset(){
        if(currentTimer!=null && !currentTimer.isDone()){
            currentTimer.cancel(false);
        }
        long timeOutMs = ThreadLocalRandom.current().nextLong(150,300);

        currentTimer = scheduler.schedule(()->{
            if (raftNode != null) {
                try {
                    raftNode.handleElectionTimeout();
                } catch (Exception e) {
                    System.err.println("Election timeout handler failed: " + e.getMessage());
                    reset(); 
                }
            }
        },
            timeOutMs, 
            TimeUnit.MILLISECONDS
        );
    }


    @Override
    public synchronized void stop(){
        if(currentTimer!=null){
            currentTimer.cancel(false);
        }
    }
    @Override
    public void shutDown(){
        scheduler.shutdownNow();
    }
    

}
