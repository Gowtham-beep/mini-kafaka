package com.minibrokerTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.minibroker.log.CorruptedMessageException;
import com.minibroker.log.Segment;

public class CorruptedMessageExceptionTest {
    @TempDir
    Path tempDir;

    private Path logPath;
    private Path indexPath;
    private Segment segment;

    @BeforeEach
    void setup() throws IOException{
        logPath = tempDir.resolve("00000000000000000000.log");
        indexPath = tempDir.resolve("00000000000000000000.index");
        segment = new Segment(0L,logPath,indexPath);
    }
    @AfterEach
    void tearDown(){
        segment.deleteFiles();
    }

    @Test
    void readingCorruptedMessage() throws IOException{
        byte[] msg0 = "Message zero".getBytes();
        long offset0 = segment.append(msg0);
        assertEquals(0, offset0);

        try(FileChannel channel = FileChannel.open(logPath, StandardOpenOption.WRITE)){
            ByteBuffer buf = ByteBuffer.allocate(1);
            buf.put((byte) 0xFF); 
            buf.flip();
            channel.write(buf, 4); 
        }

        CorruptedMessageException ex = assertThrows(
            CorruptedMessageException.class,
            () -> segment.read(offset0)
        );
        assertEquals("Message is corrupted", ex.getMessage());
    }
    

}
