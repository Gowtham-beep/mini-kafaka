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
    @Test
    void readMessageLog() throws IOException{
        MessageLog log = new MessageLog(TEST_FILE);
        byte[] message1 = "GOOOOOOOOD".getBytes();
        
        long offset = log.append(message1);
        ReadResult result = log.readMessage(offset);
        log.close();

        assertArrayEquals(message1, result.payload());
        assertEquals(offset+4+message1.length, result.nextOffset());
    }

    @Test
    void concurrentAppend() throws IOException, InterruptedException {
        MessageLog log = new MessageLog(TEST_FILE);
        int numThreads = 4;
        int messagesPerThread = 1000;
        Thread[] threads = new Thread[numThreads];

        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            threads[i] = new Thread(() -> {
                for (int j = 0; j < messagesPerThread; j++) {
                    byte[] message = ("Thread-" + threadId + "-Msg-" + j).getBytes();
                    log.append(message);
                }
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        // Reading back every message (without verification logic yet)
        long offset = 0;
        int count = 0;
        while (true) {
            ReadResult result = log.readMessage(offset);
            if (result == null) break;
            offset = result.nextOffset();
            count++;
        }
        System.out.println("Count: " + count);
        assertEquals(numThreads * messagesPerThread, count);
        
        log.close();
    }
}
