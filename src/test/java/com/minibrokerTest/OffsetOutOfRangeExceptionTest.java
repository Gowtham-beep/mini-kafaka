package com.minibrokerTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Path;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.minibroker.log.OffsetOutOfRangeException;
import com.minibroker.log.Segment;

public class OffsetOutOfRangeExceptionTest {
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
    void readingOffsetOutOfRange() {
        byte[] msg0 = "Message zero".getBytes();
        long offset0 = segment.append(msg0);
        assertEquals(0, offset0);

        long unwrittenOffset = 1;
        OffsetOutOfRangeException ex = assertThrows(
            OffsetOutOfRangeException.class,
            () -> segment.read(unwrittenOffset)
        );
        assertEquals("Offset not yet written: " + unwrittenOffset, ex.getMessage());
    }
}
