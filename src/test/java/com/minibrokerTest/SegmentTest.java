package com.minibrokerTest;

import com.minibroker.log.Segment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class SegmentTest {

    @TempDir
    Path tempDir;

    private Path logPath;
    private Path indexPath;
    private Segment segment;

    @BeforeEach
    void setUp() throws IOException {
        logPath = tempDir.resolve("00000000000000000000.log");
        indexPath = tempDir.resolve("00000000000000000000.index");
        segment = new Segment(0L, logPath, indexPath);
    }

    @Test
    void testWriteAndReadMessage() {
        byte[] payload = "Hello, Mini Broker!".getBytes();
        long offset = segment.append(payload);
        
        byte[] readPayload = segment.read(offset);
        
        assertArrayEquals(payload, readPayload, "The read payload should match the written payload.");
    }

    @AfterEach
    void tearDown() throws IOException {
        if (segment != null) {
            segment.closeChannels();
        }
    }
}
