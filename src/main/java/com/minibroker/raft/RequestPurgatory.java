package com.minibroker.raft;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import com.minibroker.raft.rpc.RecordMetaData;

public class RequestPurgatory {
    private final ConcurrentMap<Long, CompletableFuture<RecordMetaData>> pending = new ConcurrentHashMap<>();

    public void put(long offset, CompletableFuture<RecordMetaData> future) {
        pending.put(offset, future);
    }

    public void resolveAllUpTo(long offset) {
        for (long key : pending.keySet()) {
            if (key <= offset) {
                CompletableFuture<RecordMetaData> future = pending.remove(key);
                if (future != null) {
                    future.complete(new RecordMetaData(key, System.currentTimeMillis()));
                }
            }
        }
    }
}
