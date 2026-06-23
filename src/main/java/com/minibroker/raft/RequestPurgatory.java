package com.minibroker.raft;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import com.minibroker.raft.rpc.RecordMetaData;

public class RequestPurgatory {

    // Backing structure — ordered by logIndex, lock-free, supports range views
    private final ConcurrentSkipListMap<Long, CompletableFuture<RecordMetaData>> purgatoryMap = new ConcurrentSkipListMap<>();

    public void put(long logIndex, CompletableFuture<RecordMetaData> future) {
        // Registers a producer's waiting future at the exact index it was
        // appended to on the leader's local log. Non-blocking. No lock needed —
        // ConcurrentSkipListMap.put() is itself thread-safe.
        purgatoryMap.put(logIndex, future);
    }

    public void resolveAllUpTo(long commitIndex) {
        // Called by onFollowerAck() the moment leaderCommit advances.
        // Slice off every entry whose index <= commitIndex.
        ConcurrentNavigableMap<Long, CompletableFuture<RecordMetaData>> headView = purgatoryMap.headMap(commitIndex, true);

        for (Map.Entry<Long, CompletableFuture<RecordMetaData>> entry : headView.entrySet()) {
            Long index = entry.getKey();
            CompletableFuture<RecordMetaData> future = entry.getValue();

            // Atomic remove-by-value: only remove if this exact future
            // instance is still the one mapped to this index. Prevents
            // double-completion if another thread already resolved it.
            if (purgatoryMap.remove(index, future)) {
                RecordMetaData metadata = new RecordMetaData(index, System.currentTimeMillis());
                future.complete(metadata);
            }
        }
    }

    public void remove(long logIndex, CompletableFuture<RecordMetaData> future) {
        purgatoryMap.remove(logIndex, future);
    }

    public void failAll(Throwable cause) {
        // Called when this node steps down from LEADER while writes are
        // still in-flight. A write that was accepted locally but never
        // reached quorum has an undefined fate — the new leader may or
        // may not preserve it. We cannot let the producer hang forever,
        // and we cannot pretend it succeeded. Fail fast and explicit.

        // Snapshot all current entries, then clear — avoids iterating
        // while concurrently failing/removing in this single-purpose sweep.
        for (Map.Entry<Long, CompletableFuture<RecordMetaData>> entry : purgatoryMap.entrySet()) {
            Long index = entry.getKey();
            CompletableFuture<RecordMetaData> future = entry.getValue();

            if (purgatoryMap.remove(index, future)) {
                future.completeExceptionally(new NotLeaderException(
                    "Leadership lost before write at offset " + index +
                    " reached quorum. Outcome unknown — retry against new leader.", cause
                ));
            }
        }
    }

    public int size() {
        return purgatoryMap.size();
    }
}
