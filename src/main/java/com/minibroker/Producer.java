package com.minibroker;

import com.minibroker.raft.RaftNode;
import com.minibroker.raft.rpc.RecordMetaData;

import java.util.concurrent.CompletableFuture;

public class Producer {
    private final RaftNode raftNode;

    public Producer(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    /**
     * Appends a message to the cluster.
     * The returned future completes when the message is successfully replicated to a quorum.
     * 
     * @param payload The raw byte payload to send.
     * @return A CompletableFuture containing the RecordMetaData once committed.
     */
    public CompletableFuture<RecordMetaData> send(byte[] payload) {
        return raftNode.appendMessage(payload);
    }
}
