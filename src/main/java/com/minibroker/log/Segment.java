package com.minibroker.log;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

public class Segment {

    private static final long MAX_FILE_SIZE = 1024 * 1024 * 1024; 
    private static final int INDEX_INTERVAL = 4096;
    private static final int INDEX_ENTRY_SIZE = 16;


    private static final long MAX_INDEX_SIZE = (MAX_FILE_SIZE / INDEX_INTERVAL) * INDEX_ENTRY_SIZE;
    
   
    private final MappedByteBuffer logBuffer;
    private final MappedByteBuffer indexBuffer;
    private final long baseOffset;
    private final AtomicLong writePosition = new AtomicLong(0);
    private final AtomicLong messageCount = new AtomicLong(0);
    private final AtomicLong indexPosition = new AtomicLong(0);
    private final AtomicLong lastIndexBytesPos = new AtomicLong(0);
   


    private final FileChannel logFileChannel;
    private final FileChannel indexFileChannel;
    private volatile boolean isSealed = false;
   
    public Segment(long baseOffset, Path logPath, Path indexPath) throws  IOException{
        this.baseOffset = baseOffset;

        this.logFileChannel = FileChannel.open(
            logPath,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE,
            StandardOpenOption.CREATE
        );
        logFileChannel.position(MAX_FILE_SIZE-1);
        logFileChannel.write(ByteBuffer.wrap(new byte[]{0}));

        this.logBuffer =logFileChannel.map(
            FileChannel.MapMode.READ_WRITE,
            0,
            MAX_FILE_SIZE
        );

        this.indexFileChannel = FileChannel.open(
            indexPath,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE,
            StandardOpenOption.CREATE
        );
        indexFileChannel.position(MAX_INDEX_SIZE -1);
        indexFileChannel.write(ByteBuffer.wrap(new byte[]{0}));

        this.indexBuffer = indexFileChannel.map(
            FileChannel.MapMode.READ_WRITE,
            0,
            MAX_INDEX_SIZE
        );
    }
    public long append(byte[] payload){
        if(isSealed){
            throw new IllegalStateException("Segment is sealed , no writes allowed...");
        }

        CRC32 crc = new CRC32();
        crc.update(payload);
        int crc32 = (int) crc.getValue();
        int totalBytes = 4 + payload.length + 4;

        long claimedWritePos;
        while(true){
            claimedWritePos = this.writePosition.get();
            long nextWritePos = claimedWritePos + totalBytes;
            if(nextWritePos > MAX_FILE_SIZE){
                throw new IllegalStateException(
                    "Segment is full. Current position: " + claimedWritePos
                );
            }
            if(this.writePosition.compareAndSet(claimedWritePos, nextWritePos)){
                break;
            }
        }
        
        long messageIndex = this.messageCount.getAndIncrement();
        long claimedLogicalOffset = this.baseOffset + messageIndex;

        ByteBuffer buf = logBuffer.duplicate();
        buf.position((int)claimedWritePos);
        buf.putInt(payload.length);
        buf.put(payload);
        buf.putInt(crc32);

        while(true){
            long currentLast = this.lastIndexBytesPos.get();
            if((claimedWritePos - currentLast)>=INDEX_INTERVAL){
                if(this.lastIndexBytesPos.compareAndSet(currentLast, claimedWritePos)){
                    writeIndexEntry(claimedLogicalOffset,claimedWritePos);
                    break;
                }
            }else{
                break;
            }
        }

        return claimedLogicalOffset;
    }

    private void writeIndexEntry(long logicalOffset, long bytePos){
        long pos = this.indexPosition.getAndAdd(INDEX_ENTRY_SIZE);
        ByteBuffer buf = this.indexBuffer.duplicate();
        buf.position((int)pos);
        buf.putLong(logicalOffset);
        buf.putLong(bytePos);
    }

    public void seal() throws IOException{
        this.logBuffer.force();
        this.indexBuffer.force();   
        this.isSealed = true; 
        this.logFileChannel.close();
        this.indexFileChannel.close(); 
    }

    public byte[] read(long logicalOffset){
        if(logicalOffset<this.baseOffset){
            throw new OffsetOutOfRangeException("Invalid logical offset: " + logicalOffset);
        }
        
        long maxOffset = this.baseOffset + this.messageCount.get();
        if(logicalOffset >= maxOffset){
            throw new OffsetOutOfRangeException("Offset not yet written: " + logicalOffset);
        }
        
        long indexPos = this.indexPosition.get();
        long entryCount = indexPos/INDEX_ENTRY_SIZE;

        long anchorLogicalOffset = this.baseOffset;
        long anchorBytePos = 0;

        int lo = 0;
        int hi = (int)entryCount -1;

        if(entryCount>0){
            while(lo<=hi){
            int mid = lo +(hi-lo)/2;
            long entryOffset = this.indexBuffer.getLong(mid*INDEX_ENTRY_SIZE);
            if(entryOffset==logicalOffset){
                hi = mid;
                break;
            }else if(entryOffset<logicalOffset){
                lo = mid +1;
            }else{
                hi = mid -1;
            }
        }
        
        if(hi>=0){
            anchorLogicalOffset = this.indexBuffer.getLong(hi*INDEX_ENTRY_SIZE);
            anchorBytePos = this.indexBuffer.getLong((hi*INDEX_ENTRY_SIZE)+8);
        }
        }

        long currOffset = anchorLogicalOffset;
        long pos = anchorBytePos;

        while (currOffset<logicalOffset) {
            int length = this.logBuffer.getInt((int)pos);
            pos +=(4+length+4);
            currOffset++;
        }
        int length = this.logBuffer.getInt((int)pos);
        byte[] payload = new byte[length];

        for(int i=0;i<length;i++){
            payload[i] = this.logBuffer.get((int)pos+4+i);
        }
       

        int storedCRC = this.logBuffer.getInt((int)pos+4+length);
        
        CRC32 crc = new CRC32();
        crc.update(payload);
        int computedCrc32 = (int) crc.getValue();

        if(storedCRC != computedCrc32){
            throw new CorruptedMessageException("Message is corrupted");
        }

        return payload;
        
    }
    

    public long getBaseOffset() {
        return baseOffset;
    }

    public long getMessageCount() {
        return messageCount.get();
    }

    public boolean isSealed() {
        return isSealed;
    }

    public long getWritePosition() {
        return writePosition.get();
    }
}
