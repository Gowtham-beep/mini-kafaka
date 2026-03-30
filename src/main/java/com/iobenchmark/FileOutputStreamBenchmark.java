package com.iobenchmark;

import java.io.FileOutputStream;
import java.io.IOException;

public class FileOutputStreamBenchmark {
    public static final String FILE_PATH ="data/fileoutputstream_benchmark.bin";
    public static final int BYTES_TO_WRITE = 1_000_000;
    public static final byte CANARY = 0x42;
    public static void main(String[] args) throws IOException{
        byte[] data = new byte[BYTES_TO_WRITE];
        for(int i =0;i<BYTES_TO_WRITE;i++){
            data[i] = CANARY;
        }
        long startTime = System.nanoTime();
        try(FileOutputStream fos = new FileOutputStream(FILE_PATH,false)){
            fos.write(data);
        }
        long endTime = System.nanoTime();
        long  elapsedMs = (endTime- startTime)/1_000_000;

        System.out.println("FileOutputStream write — " + BYTES_TO_WRITE + " bytes");
        System.out.println("Elapsed: " + elapsedMs + " ms");
        double throughputMB = (double) BYTES_TO_WRITE / 1024 / 1024;
        double throughputPerSec = throughputMB / (elapsedMs / 1000.0);
        System.out.printf("Throughput: %.2f MB in %d ms (%.2f MB/s)%n",throughputMB, elapsedMs, throughputPerSec);
        
    }
}
