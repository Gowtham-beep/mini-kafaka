# MiniKafkaBroker

A zero-copy message broker built from scratch in Java, implementing the core mechanics of Kafka (segmented log, crash recovery) and Raft consensus (leader election, replication, safety), built as a learning project and now usable as an embedded JUnit testing library.

## What's Implemented
* **Memory-mapped I/O engine**: Uses `FileChannel` memory-mapping with lock-free offset reservation for high throughput.
* **Segmented log**: Automatically rotates log files based on size, with sparse indexing and CRC32 verification for data integrity.
* **Crash recovery**: Safely truncates conflicting log entries and recovers state across restarts.
* **Core Raft consensus**: Strict implementation of leader election, log replication, and commit/safety guarantees. *(Note: Log compaction and dynamic cluster membership are not currently implemented).*
* **TCP transport**: Real, asynchronous, non-blocking network communication via Netty.
* **Embedded API**: A native `Producer` and `Consumer` API supporting asynchronous message sending and long-polling reads.
* **JUnit 5 Extension**: Spin up an in-process replicated cluster during tests using `@EmbeddedBroker` and inject clients with `@InjectBroker`. Requires zero external dependencies.

## What This is NOT
This is an engineer's learning project built to deeply understand distributed systems internals. It is:
* **Not** a Kafka wire-protocol replacement or compatible with official Kafka clients.
* **Not** a standalone server product.
* **Not** meant to be run as a remote service in production.

## Quick Start

You can use the provided JUnit 5 extension to easily spin up a broker cluster in your integration tests. The extension automatically handles port allocation, data directory provisioning, cluster lifecycle management, and leader election wait states.

```java
package com.minibroker.testing;

import com.minibroker.Consumer;
import com.minibroker.Producer;
import com.minibroker.raft.rpc.RecordMetaData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(EmbeddedBrokerExtension.class)
@EmbeddedBroker // Defaults to replicationFactor = 1
public class EmbeddedBrokerExtensionTest {

    @InjectBroker
    private Producer producer;

    @InjectBroker
    private Consumer consumer;

    @Test
    public void testProduceAndConsume() throws Exception {
        assertNotNull(producer, "Producer should be injected");
        assertNotNull(consumer, "Consumer should be injected");

        byte[] payload = "Hello, Embedded Broker!".getBytes();

        CompletableFuture<RecordMetaData> produceFuture = producer.send(payload);
        RecordMetaData metaData = produceFuture.get(5, TimeUnit.SECONDS);

        assertNotNull(metaData);
        assertEquals(0, metaData.logicalOffset());

        CompletableFuture<java.util.List<byte[]>> consumeFuture = consumer.read(0, 1, 5000);
        java.util.List<byte[]> consumedPayloads = consumeFuture.get(5, TimeUnit.SECONDS);

        assertNotNull(consumedPayloads);
        assertEquals(1, consumedPayloads.size());
        assertArrayEquals(payload, consumedPayloads.get(0));
    }
}
```

## Project Structure
* `com.minibroker.log`: Memory-mapped segmented log with sparse indexing, CRC32 verification, and lock-free concurrency.
* `com.minibroker.raft`: Core Raft consensus state machine, leader election, log replication, and purgatories for long-polling.
* `com.minibroker.raft.rpc`: Purely asynchronous, non-blocking TCP transport layer powered by Netty.
* `com.minibroker`: High-level orchestrator classes (`Broker`, `MiniKafkaBroker`, `Producer`, `Consumer`) bridging Raft and user APIs.
* `com.minibroker.testing`: JUnit 5 extension (`@EmbeddedBroker`, `@InjectBroker`) for declarative cluster instantiation in testing environments.

## Test Coverage
The project currently has **49 passing tests** covering the critical paths of the system:
* **Pure Raft state machine**: Unit testing for leader election logic, term tracking, vote granting, and log consistency checks.
* **TCP transport & failure recovery**: Integration tests verifying cluster stability across network partitions, node crashes, and follower catch-up.
* **Disk write path**: Concurrency and stress testing the lock-free log appender, index rotation, and crash-corruption recovery.
* **Consumer long-polling**: Testing the Request/Fetch purgatory mechanisms for parked requests.

## Status / Roadmap
The core consensus, transport, and storage layers are stable and operational. Future development may include:
* **Log Compaction**: Reclaiming disk space from fully committed, obsolete log segments.
* **Dynamic Membership**: Supporting Raft cluster configuration changes without downtime.

*Detailed build-journey blog posts documenting the architecture and engineering decisions behind this project are coming soon.*
