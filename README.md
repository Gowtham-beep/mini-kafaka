# mini-kafka

A message broker built from scratch in Java — not to ship, but to understand.

This project is a deliberate reconstruction of the core ideas behind Apache Kafka: persistent storage via memory-mapped I/O, a segmented append-only log, offset-based consumer state, and fault-tolerant design. Every decision traces back to a first principle.

---

## Why build this?

There is a gap between engineers who have *used* Kafka and engineers who *understand* it.

The gap shows up when something breaks at 3am. It shows up in system design interviews. It shows up when you need to defend a trade-off to a senior engineer.

This project is an attempt to close that gap — by building the thing, breaking it deliberately, and reasoning about why it fails.

---

## What this is not

This is not a production message broker. It has no networking layer, no replication, no consumer groups. It will not handle your production traffic.

It is a learning system. The goal is that every line of code can be explained, every design decision can be defended, and every failure mode is understood before it happens.

---

## Project Structure

Three sequential projects. Each one is a complete, working system before the next begins.

```
Project 1 — The I/O Engine
Project 2 — The Log          ← in progress
Project 3 — Distribution     ← upcoming
```

---

## Project 1 — The I/O Engine ✅

**Goal:** A single-node persistent queue. Producer writes messages to disk via memory-mapped I/O. Consumer reads sequentially. Both run concurrently without corrupting each other.

### What was built

- `MessageLog` — a memory-mapped append-only log using `MappedByteBuffer`
- Lock-free offset reservation via `AtomicLong` CAS operations
- Thread-safe positional writes using `mappedBuffer.duplicate()`
- Stateless consumer — caller passes offset in, receives `ReadResult(byte[] payload, long nextOffset)` back

### Message format

```
[ 4 bytes — length ][ N bytes — payload ]
```

Length-prefixed, not delimiter-based. One read tells you exactly how far to advance. No scanning.

### Key decisions and why

**Memory-mapped I/O over FileOutputStream**
`MappedByteBuffer` writes go directly to the OS page cache. No userspace copy. The OS decides when to flush to disk. `FileOutputStream` crosses the same boundary — but measures differently. A benchmark that doesn't account for this will lie to you.

**AtomicLong CAS for offset tracking**
Two threads writing simultaneously without coordination will corrupt data silently. Not with an exception — silently. CAS reserves space atomically without a lock. The thread that wins the CAS owns that byte range exclusively.

**Stateless consumer design**
Consumer state is just an offset. The consumer doesn't hold it — the caller does. This mirrors Kafka's consumer group model exactly: the broker stores nothing about where you are. You tell it where to start. This makes replay, crash recovery, and multiple independent consumers trivial.

### Phase 4 — Deliberate failures

Before declaring Project 1 done, the system was deliberately broken:

- 4 producer threads with no synchronization → 128 of 4000 messages silently corrupted
- AtomicLong restored → 4000/4000 messages intact
- Consumer reading during a mid-write → torn write observed: 10,000 zero bytes returned as a valid message
- JVM crash mid-write → partial record at tail, undetectable without a checksum

The torn write is not fixed in Project 1. It is deferred to Project 2 where CRC32 checksums are added. Deferring it was intentional — seeing the failure first makes the fix meaningful.

---

## Project 2 — The Log 🔨

**Goal:** Extend the I/O engine into a proper segmented log. Messages append to rotating segment files. A sparse offset index maps logical offsets to physical byte positions. CRC32 checksums detect torn writes. Multiple consumers read at different offsets independently.

### Core concepts being applied

**Append-only log as a data structure**
Appending to the end of a file is fundamentally safer than modifying the middle. If the system crashes mid-append, existing data is untouched. The corruption is always at the tail. Truncate the tail — the log is consistent again.

In-place mutation requires a Write-Ahead Log to recover safely. The WAL is itself append-only. The append-only structure is the recovery primitive that everything else is built on top of.

**Log segmentation**
One infinite file creates three problems: deletion is impossible without rewriting the entire file, memory mapping an arbitrarily large file is impractical, and crash recovery scans grow unbounded.

Segments solve all three. Each segment is identified by its base offset — the logical offset of the first message in that segment. When a segment crosses the size threshold, it is sealed (flushed, closed, marked read-only) and a new segment is created. Deletion is `File.delete()` on the oldest segment — O(1), no rewriting.

```
00000000000000000000.log    ← base offset 0,    sealed
00000000000000001000.log    ← base offset 1000, sealed
00000000000000002500.log    ← base offset 2500, active
```

**Sparse offset index**
A dense index has one entry per message. At a billion messages, the index itself becomes the problem. A sparse index samples every N messages or every N bytes.

Each entry is a fixed-width 16-byte pair:
```
[ 8 bytes — logical offset ][ 8 bytes — physical byte position ]
```

Fixed-width entries mean binary search is O(1) per probe — the nth entry is always at byte `n * 16`. No scanning.

Lookup: binary search the index for the largest entry whose offset ≤ target, jump to that byte position in the log file, scan forward by reading length-prefixed records and counting offsets until you reach the target. The scan is cheap because length-prefixing makes each step a single read followed by a fixed jump.

**CRC32 checksums**
Append-only detects structural corruption — a truncated record at the tail is detectable because you read the length prefix, expect N bytes, and hit EOF. But it cannot detect torn writes where the structure looks valid but the bytes are wrong. CRC32 catches that.

Updated message format:
```
[ 4 bytes — length ][ N bytes — payload ][ 4 bytes — CRC32 ]
```

**Crash recovery**
The log file is the source of truth. The index is derived. On startup: read the last valid index entry, jump to that byte position in the log file, scan forward verifying CRC32 checksums, rebuild any missing index entries until the true end of valid data is found.

### Concurrency primitives introduced

- `ReentrantLock` — explicit locking with fairness control
- `ReadWriteLock` — multiple readers on sealed segments, exclusive writer on active segment
- `Condition` variables — blocking consumer until data arrives instead of spinning

---

## Project 3 — Distribution 📋

**Goal:** Three instances of the Project 2 log running as a cluster. Leader-follower replication. Producer writes to leader. Followers replicate. If the leader dies, a follower takes over.

Concepts: replication factor, acknowledgment modes (`acks=0`, `acks=1`, `acks=all`), ISR, leader election, split-brain prevention, CAP theorem applied to a system you actually built.

---

## Stack

- Java 21
- Maven
- JUnit 5
- Java NIO — `FileChannel`, `MappedByteBuffer`

---

## Running the tests

```bash
mvn clean test
```

---

## What this builds toward

By the end of Project 3:

- Explain from first principles why Kafka uses a segmented append-only log
- Defend every I/O design decision with OS-level reasoning
- Whiteboard a replication protocol and identify its failure modes
- Discuss CAP trade-offs against a system you actually built, not just read about
- Reason about concurrency at the level of the Java Memory Model, not just syntax

---

## Related writing

*My benchmark returned 953 MB/s. I thought I understood I/O. I was completely wrong.* — blog post written alongside Project 1, covering the benchmark design mistakes that produced a misleading number and what the corrected measurement actually revealed.
