package com.minibrokerTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.minibroker.log.SegmentedLog;

public class ConditionVariableTest {
    @TempDir
    Path tempDir;

    private SegmentedLog log;

    @BeforeEach
    public void setup() throws Exception{
        log = new SegmentedLog(0L, tempDir,1024*1024);
    }
    @AfterEach
    public void tearDown(){
        log.close();
    }

    @Test
    public void testConsumerBlocksUntilProducerWrites() throws Exception{
        byte[] firstMessage = "historical-data".getBytes(StandardCharsets.UTF_8);
        long offsset0 = log.append(firstMessage);
        assertEquals(0, offsset0,"First offset should be 0");

        CompletableFuture<byte[]> futureRead = CompletableFuture.supplyAsync(() ->{
            try{
                return log.read(1);
            }catch(InterruptedException e){
                Thread.currentThread().interrupt();
                throw new RuntimeException("Consumer was wrongly interrupted",e);
            }
        });
        Thread.sleep(100);
        assertFalse(futureRead.isDone(),"CRITICAL FAILURE: Consumer did not block! It either spun out or crashed");

        byte[] futureMessage = "future-data".getBytes(StandardCharsets.UTF_8);
        long offset1 = log.append(futureMessage);
        assertEquals(1,offset1,"Second offset should be 1");

        try{
            byte[] readData = futureRead.get(2,TimeUnit.SECONDS);
            String result = new String(readData,StandardCharsets.UTF_8);
            assertEquals("future-data", result, "Consumer woke up but read the wrong data.");

        }catch(TimeoutException e){
             fail("CRITICAL FAILURE: Producer appended data, but the Consumer never woke up! The signal dropped.");
        }
    }
}
