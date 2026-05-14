package com.minibrokerTest;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.minibroker.log.SegmentedLog;

public class SegmentedLogTest {
    @TempDir
    Path tempDir;

    private Path logPath;
    private SegmentedLog log;

    @BeforeEach
    void setup() throws IOException{
         logPath = tempDir.resolve("test.log");
         log = new SegmentedLog(0L, logPath);
    }

    @AfterEach
    void tearDown() throws IOException{
        log.close();
    }
    
    @Test
    void testWriteAndReadMessage(){
        byte[] msg0 = "Hello Storage Engine".getBytes();
        byte[] msg1 = "Message Two".getBytes();
        byte[] msg2 = "MThe final message in the srquence".getBytes();

        long offset0 = log.append(msg0);
        long offset1 = log.append(msg1);
        long offset2 = log.append(msg2);

        assertEquals(0L,offset0);
        assertEquals(1L, offset1);
        assertEquals(2L, offset2);

        byte[] retrivedMessage2 = log.read(offset2);
        byte[] retrivedMessage0 = log.read(offset0);
        byte[] retrivedMessage1 = log.read(offset1);

        assertArrayEquals(msg2, retrivedMessage2);
        assertArrayEquals(msg0, retrivedMessage0);
        assertArrayEquals(msg1, retrivedMessage1);

    }
}
