package com.iobenchmark;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

public class MessageLogTest {
    private static final String TEST_FILE = "data/test_log.bin";

    @AfterEach
    public void cleanup() throws IOException {
        Files.deleteIfExists(Path.of(TEST_FILE));
    }

    @Test
    void appendReturnDifferentOffset() throws IOException{
        MessageLog log = new MessageLog(TEST_FILE);

        byte[] message1 = "Hello".getBytes();
        byte[] message2 = "World".getBytes();

        long offset1 = log.append(message1);
        long offset2 = log.append(message2);

        log.close();
        
        assertEquals(0, offset1);
        assertEquals(9, offset2);
    }


}
