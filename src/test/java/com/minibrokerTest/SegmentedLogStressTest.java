package com.minibrokerTest;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.minibroker.log.SegmentedLog;

public class SegmentedLogStressTest {
    @TempDir
    Path tempDir;

    private Path logPath;
    private SegmentedLog log;

    @BeforeEach
    void setup() throws IOException{
        logPath = tempDir.resolve("test.log");
    }
    @AfterEach
    void tearDown() throws IOException{
        if(log!=null){
            log.close();
        }
    }
    @Test
    void testMillionMessageConcurrency() throws Exception{
        int threadCount = 100;
        int messagePerThread = 10_000;
        long totalMessages = (long) threadCount * messagePerThread;

        long maxSegmentSize = 10*1024*1024L;

        log = new SegmentedLog(0L, logPath,maxSegmentSize);

        byte[] payload = new byte[100];
        for(int i=0;i<payload.length;i++){
            payload[i] = (byte) (i%256);
        }
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CyclicBarrier startingGun = new CyclicBarrier(threadCount);
        CountDownLatch finishLine = new CountDownLatch(threadCount);
        List<Exception> threadExceptions = new CopyOnWriteArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    // Hold all threads at the starting line for maximum burst contention
                    startingGun.await(); 
                    
                    for (int j = 0; j < messagePerThread; j++) {
                        log.append(payload);
                    }
                } catch (Exception e) {
                    threadExceptions.add(e);
                } finally {
                    finishLine.countDown();
                }
            });
        }

         boolean finished = finishLine.await(30, TimeUnit.SECONDS);
        executor.shutdown();
        assertTrue(finished, "Test timed out! Possible deadlock in the rotation lock or CAS loop.");
        
        if (!threadExceptions.isEmpty()) {
            threadExceptions.get(0).printStackTrace();
            fail("One or more producer threads threw an exception. First exception: " + threadExceptions.get(0).getMessage());
        }
        for (long offset = 0; offset < totalMessages; offset++) {
            try {
                byte[] retrieved = log.read(offset);
                assertArrayEquals(payload, retrieved, "Payload corrupted at offset: " + offset);
            } catch (Exception e) {
                fail("Failed to read offset " + offset + ". Engine data is corrupted or missing. Error: " + e.getMessage());
            }
        }
    }
}
