package com.iobenchmark;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;



public class MessageLog {
 
    private static final long MAX_FILE_SIZE = 1024 * 1024 * 1024; 
    private final FileChannel fileChannel;
    private MappedByteBuffer mappedByteBuffer;
    private final AtomicLong writePosition = new AtomicLong(0);

    public MessageLog(String filepath) throws IOException{
        this.fileChannel = FileChannel.open(
          Path.of(filepath),
          StandardOpenOption.READ,
          StandardOpenOption.WRITE,
          StandardOpenOption.CREATE  
        );

        fileChannel.position(MAX_FILE_SIZE - 1);
        fileChannel.write(ByteBuffer.wrap(new byte[]{0}));

        this.mappedByteBuffer = fileChannel.map(
            FileChannel.MapMode.READ_WRITE,
            0, 
            MAX_FILE_SIZE
        );
    }

    public long append( byte[] message) throws InterruptedException{
        int totalSize = 4 + message.length;
        long reservedOffset;
        while(true){
            long current = writePosition.get();
            long next = current + totalSize;

            if (next > MAX_FILE_SIZE) {
                throw new IllegalStateException(
                    "Log is full. Current position: " + current 
                );
            }
            if (writePosition.compareAndSet(current, next)) {
                reservedOffset = current;
                break;
            }
        }
        ByteBuffer view = mappedByteBuffer.duplicate();
        view.position((int)reservedOffset);
        view.putInt(message.length);
        Thread.sleep(10);
        view.put(message);
        return reservedOffset;
    }

    public ReadResult readMessage(long offset){
        if(offset>=writePosition.get()){
            return null;
        }
        ByteBuffer slice = mappedByteBuffer.duplicate();
        slice.position((int)offset);
        int messageLength = slice.getInt();
        byte[] payload = new byte[messageLength];
        slice.get(payload);

        long nextOffset = offset + 4 +  messageLength;
        return new ReadResult(payload,nextOffset);
    }

    public void close() throws IOException{
        mappedByteBuffer.force();
        fileChannel.close();
    }

}
