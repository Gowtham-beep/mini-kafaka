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
   
    private final MappedByteBuffer logBuffer;
    private final MappedByteBuffer indexBuffer;
    private final long baseOffset;
    private final AtomicLong writePosition = new AtomicLong(0);
    private final AtomicLong messageCount = new AtomicLong(0);
    private final AtomicLong indexPosition = new AtomicLong(0);
    private final AtomicLong lastIndexBytesPos = new AtomicLong(0);
    private static final int INDEX_INTERVAL = 4096;
    private static final int INDEX_ENTRY_SIZE = 16;

    private final FileChannel logFileChannel;
    private final FileChannel indexFileChannel;
    private volatile boolean isSealed = false;
    private static final long MAX_FILE_SIZE = 1024 * 1024 * 1024; 

    private static final long MAX_INDEX_SIZE = (MAX_FILE_SIZE / INDEX_INTERVAL) * INDEX_ENTRY_SIZE;
    
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
    public long append(byte[] payload) throws InterruptedException{
        if(isSealed){
            throw new IllegalStateException("Segment is sealed , no writes allowed...");
        }
        CRC32 crc = new CRC32();
        crc.update(payload);
        int crc32 = (int) crc.getValue();


        int totalBytes = 4 + payload.length + 4;

        synchronized (this) {
            if(isSealed){
            throw new IllegalStateException("Segment is sealed , no writes allowed...");
        }
        long currentWritePos = this.writePosition;
        if(currentWritePos + totalBytes>=MAX_FILE_SIZE){
            return -1;
            //signaling the segmentLog
        }
        ByteBuffer buf = logBuffer.duplicate();
        buf.position((int)currentWritePos);
        buf.putInt(payload.length);
        buf.put(payload);
        buf.putInt(crc32)

        long logicalOffset = this.baseOffset + this.messageCount;
        this.writePosition+=totalBytes;
        this.messageCount++;
        }
        if(currentWritePos - this.lastIndexBytesPos>=INDEX_INTERVAL){
            writeIndexEntry(logicalOffset,currentWritePos);
            this.lastIndexBytesPos=currentWritePos;
        }

        return logicalOffset;
    }

}
