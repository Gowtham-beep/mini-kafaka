package com.minibroker.log;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.zip.CRC32;




public class SegmentedLog {
    private final Path baseDir; 
    private final long initialOffset;
    private final List<Segment> segments;
    private volatile Segment currentSegment;
    private final ReentrantLock reentrantLock;
    private final long maxFileSize;
    private volatile boolean closed = false;
    private final ReadWriteLock routingLock = new ReentrantReadWriteLock();

    private final ReentrantLock newDataLock = new ReentrantLock();
    private final Condition newDataCondition = newDataLock.newCondition();
    private final AtomicInteger waitingConsumers = new AtomicInteger(0);


    public SegmentedLog(long initialOffset, Path baseDir, long maxFileSize) throws IOException{
        this.baseDir = baseDir;
        this.initialOffset = initialOffset;
        this.segments = new CopyOnWriteArrayList<>();
        this.reentrantLock = new ReentrantLock(false);
        this.maxFileSize = maxFileSize;

        if(!Files.exists(baseDir)){
            Files.createDirectories(baseDir);
        }

        recoveryServiceProtocol();

    }

    public SegmentedLog(long initialOffset, Path baseDir) throws IOException {
        this(initialOffset, baseDir, Segment.DEFAULT_MAX_FILE_SIZE);
    }
    
    private Segment createNewSegment(long baseOffset) throws IOException{
        String baseName = String.format("%020d", baseOffset);
        Path logPath = baseDir.resolve(baseName + ".log");
        Path indexPath = baseDir.resolve(baseName + ".index");

        return new Segment(baseOffset, logPath, indexPath, maxFileSize);
    }
//hot path 
    public long append(byte[] payload){
        if (closed) {
            throw new IllegalStateException("Log is closed");
        }
        long logicalOffset;
        while(true){
            Segment activeSegment = this.currentSegment;
            try{
                logicalOffset =  activeSegment.append(payload);
                break;

            }catch(IllegalStateException e){
                reentrantLock.lock();
                try{
                    if (closed) {
                        throw new IllegalStateException("Log was closed during rotation");
                    }
                    if(this.currentSegment == activeSegment){
                        rotate(activeSegment);
                    }
                }finally{
                    reentrantLock.unlock();
                }
            }
        }
        if(waitingConsumers.get()>0){
            newDataLock.lock();
            try{
                newDataCondition.signalAll();
            }finally{
                newDataLock.unlock();
            }
        }
        return logicalOffset;
    }
//cold path
    private void rotate(Segment oldSegment){
        try{
            oldSegment.seal();
            long newBaseOffset = oldSegment.getBaseOffset() + oldSegment.getMessageCount();
            Segment newSegment = createNewSegment(newBaseOffset);
            this.segments.add(newSegment);
            this.currentSegment = newSegment;

        }catch( IOException e){
            throw new RuntimeException("CRITICAL IO ERROR: Failed to rotate segment",e);
        }
    }


    public byte[] read(long logicalOffset) throws InterruptedException{
        if(logicalOffset<=getHighestCommittedOffset()){
            return doRead(logicalOffset);
        }
        newDataLock.lock();
        try{
            waitingConsumers.incrementAndGet();
                try{
                    while(logicalOffset>getHighestCommittedOffset()){
                        newDataCondition.await();
                    }
                }finally{
                    waitingConsumers.decrementAndGet();
                }
        }finally{
            newDataLock.unlock();
        }
        return doRead(logicalOffset);
        }


    private byte[] doRead( long logicalOffset){
        routingLock.readLock().lock();
        try{
            Segment targetSegment = findSegment(logicalOffset);
        if(targetSegment == null){
            throw new OffsetOutOfRangeException("Offset " + logicalOffset + " is out of range");
        }
        return targetSegment.read(logicalOffset);
        }finally{
            routingLock.readLock().unlock();
        }
    }

    private Segment findSegment(long logicalOffset){
        int lo =0;
        int hi = segments.size() -1;
        Segment floorSegment = null;

        while(lo<=hi){
            int mid = lo + (hi-lo)/2;
            Segment midSegment = segments.get(mid);
            long midBaseOffset = midSegment.getBaseOffset();
            if(midBaseOffset == logicalOffset){
                return midSegment;
            }else if (midBaseOffset< logicalOffset){
                floorSegment = midSegment;
                lo = mid + 1;
            }else{
                hi = mid -1;

            }
        }
        if(floorSegment!=null){
            long maxOffsetInSegment = floorSegment.getBaseOffset()+floorSegment.getMessageCount();
            if(logicalOffset< maxOffsetInSegment){
                return floorSegment;
            }
        }
        return null; 
    }

    private long getHighestCommittedOffset(){
        if(segments.isEmpty()){
            return -1;
        }
        Segment lastSegment = segments.get(segments.size() - 1);
        return lastSegment.getBaseOffset() + lastSegment.getMessageCount() - 1;
    }

    public void close(){
        reentrantLock.lock();
        try {
            closed = true;
            for(Segment segment : segments){
                if(!segment.isSealed()){
                    segment.seal();
                }else{
                    segment.closeChannels();
                }
            }
        }catch(IOException e){
            System.err.println("Error closing segment: " + e.getMessage());
        }
         finally {
            reentrantLock.unlock();
        }

    }

    public void delete(long retentionOffset){
        List<Segment> expiredSegments = new ArrayList<>();

        for(int i=0;i<segments.size()-1;i++){

            Segment curr = segments.get(i);
            Segment next = segments.get(i+1);

            if(next.getBaseOffset() <=retentionOffset){
                expiredSegments.add(curr);
            }else{
                break;
            }
        }
        if(expiredSegments.isEmpty()){
            return;
        }


        routingLock.writeLock().lock();
        try {
            this.segments.removeAll(expiredSegments);
        } finally{
            routingLock.writeLock().unlock();
        }


        for(Segment segment: expiredSegments ){
            try {
                segment.deleteFiles();
            } catch (Exception e) {
                System.err.println("Failed to delete segment: " + e.getMessage());
            }
            
        }

    }

    public void recoveryServiceProtocol() throws IOException{
        if(!Files.exists(baseDir)){
            Files.createDirectories(baseDir);
        }

        this.segments.clear();

        List<Path> logFiles;
        try (var stream = Files.list(baseDir)) {
            logFiles = stream
                .filter(p -> p.toString().endsWith(".log"))
                .sorted()
                .collect(Collectors.toList());
        }

        if(logFiles.isEmpty()){
            this.currentSegment = createNewSegment(initialOffset);
            this.segments.add(this.currentSegment);
            return;
        }

        for(int i=0;i<logFiles.size();i++){
            Path logFile = logFiles.get(i);
            long baseOffset = extractBaseOffset(logFile);
            Path indexFile = baseDir.resolve(String.format("%020d.index", baseOffset));

            if (!Files.exists(indexFile)) {
                System.err.println("Warning: Index missing for segment " + baseOffset + ". Rebuilding from log...");
                rebuildIndex(logFile, indexFile, baseOffset);
            }

            RecoveryState recoveryState = scanSegment(logFile);
            Segment recoveredSegment = createNewSegment(baseOffset);
            recoveredSegment.recoverState(recoveryState.messageCount(), recoveryState.writePosition());
            if(i < logFiles.size() - 1){
                recoveredSegment.seal();
            }
            this.segments.add(recoveredSegment);
        }

        this.currentSegment = this.segments.get(this.segments.size() - 1);
    }

    private RecoveryState scanSegment(Path logFile) throws IOException {
        long validWritePosition = 0;
        long validMessageCount = 0;

        try (FileChannel logChannel = FileChannel.open(logFile, StandardOpenOption.READ, StandardOpenOption.WRITE)){
            ByteBuffer headerBuffer = ByteBuffer.allocate(Integer.BYTES);
            CRC32 crc32 = new CRC32();

            while(validWritePosition + Integer.BYTES <= maxFileSize){
                headerBuffer.clear();
                logChannel.position(validWritePosition);
                if(logChannel.read(headerBuffer) < Integer.BYTES) break;
                headerBuffer.flip();
                int messageLength = headerBuffer.getInt();

                if(messageLength <= 0) break;
                long recordSize = Integer.BYTES + (long) messageLength + Integer.BYTES;
                if(validWritePosition + recordSize > maxFileSize) break;

                ByteBuffer payloadBuffer = ByteBuffer.allocate(messageLength);
                if(logChannel.read(payloadBuffer) < messageLength) break;
                payloadBuffer.flip();

                ByteBuffer crcBuffer = ByteBuffer.allocate(Integer.BYTES);
                if(logChannel.read(crcBuffer) < Integer.BYTES) break;
                crcBuffer.flip();
                long storedCrc = Integer.toUnsignedLong(crcBuffer.getInt());

                byte[] payload = new byte[messageLength];
                payloadBuffer.get(payload);
                crc32.reset();
                crc32.update(payload);
                if(crc32.getValue() != storedCrc){
                    System.err.println("Corruption detected at physical position " + validWritePosition + ". Halting scan.");
                    break;
                }

                validWritePosition += recordSize;
                validMessageCount++;
            }

            if(logChannel.size() > validWritePosition){
                logChannel.truncate(validWritePosition);
            }
        }

        return new RecoveryState(validMessageCount, validWritePosition);
    }

    private long extractBaseOffset(Path logFile) {
        String fileName = logFile.getFileName().toString();
        return Long.parseLong(fileName.replace(".log", ""));
    }

    private void rebuildIndex(Path logFile, Path indexFile, long baseOffset) throws IOException {
        // Placeholder for sequential log scan to drop anchors every N bytes.
        // Similar to the Phase 4 forward scan, but writes to the index file.
        Files.createFile(indexFile);
    }

    private record RecoveryState(long messageCount, long writePosition) {}
}
