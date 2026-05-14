package com.minibrokerTest;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

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

    @Test
    void testSegmentRotationOnSizeLimit() throws IOException{
        long maxSegmentSize = 250;
        log = new SegmentedLog(0L,logPath,maxSegmentSize);

        byte[] payload = new byte[100];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) i;
        }
        long offset0 = log.append(payload);
        assertEquals(0, offset0);

        long offset1 = log.append(payload);
        assertEquals(1,offset1);

        long offset2 = log.append(payload);
        assertEquals(2,offset2);

        long offset3 = log.append(payload);
        assertEquals(3,offset3);

        assertArrayEquals(payload, log.read(3));
        assertArrayEquals(payload, log.read(0));
        assertArrayEquals(payload, log.read(2));
        assertArrayEquals(payload, log.read(1));

        long fileCount;
        try(Stream<Path> files = Files.list(logPath)){
            fileCount = files.filter(p->p.toString().endsWith(".log")).count();
        }
        assertEquals(2, fileCount, "There should be exactly two physical .log files on the disk.");   
    }
}
