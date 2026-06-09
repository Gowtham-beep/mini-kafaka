package com.minibrokerTest;

import com.minibroker.log.OffsetOutOfRangeException;
import com.minibroker.log.SegmentedLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class SegmentedLogDeletionRaceTest {

    @TempDir
    Path tempDir;

    private SegmentedLog log;

    @BeforeEach
    public void setUp() throws Exception {
        
        log = new SegmentedLog(0, tempDir, 1024);
    }

    @AfterEach
    public void tearDown() {
        if (log != null) {
            log.close(); 
        }
    }

    @Test
    public void testReadWhileSegmentDeletedMidFlight() throws Exception {
        
        byte[] payload = "hello world".getBytes(StandardCharsets.UTF_8);
        long offset = 0;
        
        for (int i = 0; i < 200; i++) {
            offset = log.append(payload);
        }
        final long lastOffset = offset;

        
        int readerCount = 19;
        int totalThreads = readerCount + 1; // 19 Readers + 1 Janitor

        ExecutorService executor = Executors.newFixedThreadPool(totalThreads);
        CountDownLatch startGun = new CountDownLatch(1);
        CountDownLatch finishLine = new CountDownLatch(totalThreads);

        AtomicInteger successfulReads = new AtomicInteger(0);
        AtomicInteger expectedExpiredErrors = new AtomicInteger(0);
        List<Exception> fatalExceptions = new CopyOnWriteArrayList<>();

      
        for (int i = 0; i < readerCount; i++) {
            executor.submit(() -> {
                try {
                    startGun.await();
                    
                    for (int j = 0; j < 5000; j++) {
                        try {
                            byte[] data = log.read(0);
                            
                            
                            if (data != null && data.length > 0) {
                                successfulReads.incrementAndGet();
                            }
                        } catch (OffsetOutOfRangeException e) {
                            
                            expectedExpiredErrors.incrementAndGet();
                        } catch (Exception e) {
                         
                            fatalExceptions.add(e);
                        }
                    }
                } catch (Exception e) {
                    fatalExceptions.add(e);
                } finally {
                    finishLine.countDown();
                }
            });
        }

        
        executor.submit(() -> {
            try {
                startGun.await();
                
               
                Thread.sleep(100); 
                
                
                log.delete(lastOffset);

            } catch (Exception e) {
                fatalExceptions.add(e);
            } finally {
                finishLine.countDown();
            }
        });

        
        startGun.countDown();
        
        
        boolean completedCleanly = finishLine.await(15, TimeUnit.SECONDS);
        executor.shutdownNow();

        assertTrue(completedCleanly, "Test timed out. The ReadWriteLock likely caused a deadlock.");

        if (!fatalExceptions.isEmpty()) {
            fatalExceptions.get(0).printStackTrace();
            fail("Encountered " + fatalExceptions.size() + " fatal exceptions. First exception printed above.");
        }

        System.out.println("Successful pre-deletion reads: " + successfulReads.get());
        System.out.println("Expected post-deletion rejections: " + expectedExpiredErrors.get());

        assertTrue(successfulReads.get() > 0, "Readers never successfully read the file before it was deleted.");
        assertTrue(expectedExpiredErrors.get() > 0, "The Janitor never successfully deleted the file, or readers didn't catch the update.");
    }
}