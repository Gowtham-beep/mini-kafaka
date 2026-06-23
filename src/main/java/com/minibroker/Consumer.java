package com.minibroker;

import com.minibroker.log.SegmentedLog;
import com.minibroker.raft.FetchPurgatory;
import com.minibroker.raft.RaftNode;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public class Consumer {
    private final RaftNode raftNode;
    private final Executor consumerIoExecutor;

    public Consumer(RaftNode raftNode, Executor consumerIoExecutor) {
        this.raftNode = raftNode;
        this.consumerIoExecutor = consumerIoExecutor;
    }

    /**
     * Reads up to maxBatchSize records starting from the given offset.
     * Uses long-polling: if the requested offset is not yet committed, it parks the request
     * and waits up to timeoutMs for the offset to become committed.
     * 
     * @param offset The starting log index to read.
     * @param maxBatchSize The maximum number of records to return.
     * @param timeoutMs The maximum time to wait if the data is not yet available.
     * @return A CompletableFuture containing a list of record payloads.
     */
    public CompletableFuture<List<byte[]>> read(long offset, int maxBatchSize, long timeoutMs) {
        if (offset <= raftNode.getCommitIndex()) {
            return CompletableFuture.completedFuture(doRead(offset, maxBatchSize));
        }

        var future = new CompletableFuture<Void>();
        FetchPurgatory fetchPurgatory = raftNode.getFetchPurgatory();
        fetchPurgatory.put(offset, future);

        // The Rescue Post-Check: Did commitIndex advance while we were parking?
        // Invariant: commitIndex is strictly monotonic. If it has advanced past our offset,
        // we might have missed the wakeAllUpTo() signal. So we rescue ourselves.
        if (offset <= raftNode.getCommitIndex()) {
            if (future.complete(null)) {
                fetchPurgatory.remove(offset, future);
            }
        }

        return future.orTimeout(timeoutMs, TimeUnit.MILLISECONDS)
            .handleAsync((res, ex) -> {
                if (ex != null) {
                    if (ex instanceof java.util.concurrent.TimeoutException) {
                        fetchPurgatory.remove(offset, future);
                        // On timeout, return empty batch to simulate long-poll empty response
                        return new ArrayList<byte[]>();
                    }
                    if (ex instanceof RuntimeException) {
                        throw (RuntimeException) ex;
                    }
                    throw new RuntimeException((Throwable) (ex instanceof java.util.concurrent.CompletionException ? ex.getCause() : ex));
                }
                
                // If the future completed normally, the offset is now committed!
                return doRead(offset, maxBatchSize);
            }, consumerIoExecutor);
    }

    private List<byte[]> doRead(long startingOffset, int maxBatchSize) {
        SegmentedLog log = raftNode.getLog();
        long commitIndex = raftNode.getCommitIndex();
        
        long endBoundary = Math.min(startingOffset + maxBatchSize - 1, commitIndex);
        List<byte[]> batch = new ArrayList<>();
        
        for (long i = startingOffset; i <= endBoundary; i++) {
            try {
                // SegmentedLog.read() might still contain old blocking logic,
                // but since we strictly check i <= commitIndex <= lastOffset,
                // it shouldn't block.
                batch.add(log.read(i));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted during read", e);
            }
        }
        
        return batch;
    }
}
