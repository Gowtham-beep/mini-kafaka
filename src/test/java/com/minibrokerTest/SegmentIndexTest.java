package com.minibrokerTest;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.minibroker.log.Segment;

public class SegmentIndexTest {

    @TempDir
    Path tempDir;

    private Path logPath;
    private Path indexPath;
    private Segment segment;
    
    @BeforeEach
    void setup() throws IOException{
      
        logPath = tempDir.resolve("000000000000000000000.log");
        indexPath = tempDir.resolve("00000000000000000000.index");
        segment = new Segment(0L,logPath,indexPath);
    }

    @AfterEach
    void tearDown(){
        segment.deleteFiles();
    }

    @Test
    void TestSparseIndexJumpBypassCorruptedData() throws IOException{
        byte[] msg0 = "Message zero".getBytes();
        long offset0 = segment.append(msg0);
        assertEquals(0, offset0);

        byte[] msg1 = new byte[4104];
        long offset1 = segment.append(msg1);
        assertEquals(1, offset1);

        byte[] msg2 = "Message 2".getBytes();
        long offset2 = segment.append(msg2);
        assertEquals(2, offset2);

        try(FileChannel channel = FileChannel.open(logPath, StandardOpenOption.WRITE)){
            ByteBuffer buf = ByteBuffer.allocate(4);
            buf.putInt(Integer.MAX_VALUE - 100);
            buf.flip();
            channel.write(buf,0);
        }

        byte[] retrivedMessage2 = segment.read(offset2);
        assertArrayEquals(msg2,retrivedMessage2);
    }

}
