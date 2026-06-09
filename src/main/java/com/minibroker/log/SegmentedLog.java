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
        return append(0L,payload);
    }
    public long append(long term ,byte[] payload){
        if (closed) {
            throw new IllegalStateException("Log is closed");
        }
        long logicalOffset;
        while(true){
            Segment activeSegment = this.currentSegment;
            try{
                logicalOffset =  activeSegment.append(term,payload);
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

    public long getTermAtOffset(long logicalOffset){
        routingLock.readLock().lock();
        try{
            Segment traSegment = findSegment(logicalOffset);
            if(traSegment == null){
                throw new OffsetOutOfRangeException("offset" + logicalOffset + "is out of range");
            }
            return traSegment.getTermAtOffset(logicalOffset);
        }finally{
            routingLock.readLock().unlock();
        }
    }

    public long getLastOffset(){
        if(segments.isEmpty()){
            return -1;
        }
        Segment lastSegment = segments.get(segments.size() -1);
        long  messageCount = lastSegment.getMessageCount();
        if(messageCount ==0){
            if(segments.size()>1){
                Segment prevSegment = segments.get(segments.size()-2);
                return prevSegment.getBaseOffset() + prevSegment.getBaseOffset()-1;
            }
            return -1;
        }
        return lastSegment.getBaseOffset() + messageCount -1;
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
        //phase 1
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
        //phase 2
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
            ByteBuffer headerBuffer = ByteBuffer.allocate(12);
            CRC32 crc32 = new CRC32();

            while(validWritePosition + 12 <= maxFileSize){
                headerBuffer.clear();
                logChannel.position(validWritePosition);
                if(logChannel.read(headerBuffer) < 12) break;
                headerBuffer.flip();

                long term =headerBuffer.getLong();
                int messageLength = headerBuffer.getInt();

                if(messageLength <= 0) break;
                long recordSize = 16L + messageLength;
                if(validWritePosition + recordSize > maxFileSize) break;

                ByteBuffer payloadBuffer = ByteBuffer.allocate(messageLength);
                if(logChannel.read(payloadBuffer) < messageLength) break;
                payloadBuffer.flip();

                ByteBuffer crcBuffer = ByteBuffer.allocate(Integer.BYTES);
                if(logChannel.read(crcBuffer) < Integer.BYTES) break;
                crcBuffer.flip();
                long storedCrc = Integer.toUnsignedLong(crcBuffer.getInt());

                ByteBuffer crcCalBuffer = ByteBuffer.allocate(8+messageLength);
                crcCalBuffer.putLong(term);
                crcCalBuffer.put(payloadBuffer);

                crc32.reset();
                crcCalBuffer.flip();
                crc32.update(crcCalBuffer);
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
        final int ANCHOR_INTERVAL = 4096; // Drop anchors every 4KB (matches Segment.INDEX_INTERVAL)
        final int INDEX_ENTRY_SIZE = 16;  // 2 longs: logical offset (8 bytes) + byte position (8 bytes)
        
        try (FileChannel logChannel = FileChannel.open(logFile, StandardOpenOption.READ);
             FileChannel indexChannel = FileChannel.open(indexFile, 
                StandardOpenOption.WRITE, 
                StandardOpenOption.CREATE, 
                StandardOpenOption.TRUNCATE_EXISTING
            )) {
            
            ByteBuffer headerBuffer = ByteBuffer.allocate(12);
            CRC32 crc32 = new CRC32();
            long validWritePosition = 0;
            long messageCount = 0;
            long lastAnchorPosition = 0;
            ByteBuffer indexWriter = ByteBuffer.allocate(INDEX_ENTRY_SIZE);
            
            while (validWritePosition +12 <= maxFileSize) {
                headerBuffer.clear();
                logChannel.position(validWritePosition);
                if (logChannel.read(headerBuffer) < 12) break;
                headerBuffer.flip();

                long term = headerBuffer.getLong();
                int messageLength = headerBuffer.getInt();
                
                if (messageLength <= 0) break;
                long recordSize = 16L +messageLength + Integer.BYTES;
                if (validWritePosition + recordSize > maxFileSize) break;
                
                ByteBuffer payloadBuffer = ByteBuffer.allocate(messageLength);
                if (logChannel.read(payloadBuffer) < messageLength) break;
                payloadBuffer.flip();

                byte[] payload = new byte[messageLength];
                payloadBuffer.get(payload);
                
                ByteBuffer crcBuffer = ByteBuffer.allocate(Integer.BYTES);
                if (logChannel.read(crcBuffer) < Integer.BYTES) break;
                crcBuffer.flip();
                long storedCrc = Integer.toUnsignedLong(crcBuffer.getInt());
                
                ByteBuffer crcCalBuffer = ByteBuffer.allocate(8 + messageLength);
                crcCalBuffer.putLong(term);
                crcCalBuffer.put(payload);
                crc32.reset();
                crcCalBuffer.flip();
                crc32.update(crcCalBuffer);
                if (crc32.getValue() != storedCrc) {
                    System.err.println("Corruption detected at physical position " + validWritePosition + ". Halting index rebuild.");
                    break;
                }
                
                // Write index anchor every ANCHOR_INTERVAL bytes
                if (validWritePosition - lastAnchorPosition >= ANCHOR_INTERVAL) {
                    long logicalOffset = baseOffset + messageCount;
                    indexWriter.clear();
                    indexWriter.putLong(logicalOffset);
                    indexWriter.putLong(validWritePosition);
                    indexWriter.flip();
                    indexChannel.write(indexWriter);
                    lastAnchorPosition = validWritePosition;
                }
                
                validWritePosition += recordSize;
                messageCount++;
            }
            
            indexChannel.force(true);  // Ensure all index data is written to disk
        }
    }

    private record RecoveryState(long messageCount, long writePosition) {}
}
