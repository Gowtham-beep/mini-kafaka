package com.minibroker.raft;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class FetchPurgatory {

    // 1:N mapping: One offset can have multiple independent consumers parked on it.
    private final ConcurrentSkipListMap<Long, ConcurrentLinkedQueue<CompletableFuture<Void>>> purgatoryMap = new ConcurrentSkipListMap<>();

    public void put(long offset, CompletableFuture<Void> future) {
        purgatoryMap.computeIfAbsent(offset, k -> new ConcurrentLinkedQueue<>()).add(future);
    }

    // INVARIANT: The post-check pattern used by consumers relies fundamentally on 
    // the assumption that commitIndex only ever moves forward (is strictly monotonic).
    // If commitIndex could move backward, a consumer could check commitIndex, see it 
    // hasn't reached its offset, park in this map, and miss the wake-up entirely.
    public void wakeAllUpTo(long commitIndex) {
        ConcurrentNavigableMap<Long, ConcurrentLinkedQueue<CompletableFuture<Void>>> headView = purgatoryMap.headMap(commitIndex, true);
        for (Long index : headView.keySet()) {
            // 1. Atomically remove the ENTIRE queue from the map
            // This prevents the memory leak of empty queues left sitting in the map forever.
            Queue<CompletableFuture<Void>> detachedQueue = purgatoryMap.remove(index);
            
            // 2. Drain it in isolation
            if (detachedQueue != null) {
                for (CompletableFuture<Void> future : detachedQueue) {
                    future.complete(null);
                }
            }
        }
    }

    public void remove(long offset, CompletableFuture<Void> future) {
        // Called when a future times out or rescues itself.
        // We use computeIfPresent to atomically remove the map entry if the queue drops to zero size.
        // This prevents a TOCTOU race against a concurrent put().
        purgatoryMap.computeIfPresent(offset, (k, queue) -> {
            queue.remove(future);
            return queue.isEmpty() ? null : queue; 
        });
    }
}
