package com.iobenchmark;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Arrays;
import java.nio.file.StandardOpenOption;


public class MappedBufferBenchmark {

    public static final String FILE_PATH ="data/mapped_benchmark.bin";
    public static final int BYTES_TO_WRITE = 1_000_000;
    public static final byte CANARY = 0x42;
    public static void main(String[] args) throws IOException{
        byte[] data = new byte[] {CANARY};
        Arrays.fill(data, CANARY);

        long startTime = System.nanoTime();
        try(
            FileChannel channel = FileChannel.open(
                Path.of(FILE_PATH),
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE
            )){
            channel.position(BYTES_TO_WRITE -1);
            channel.write(java.nio.ByteBuffer.wrap(new byte[]{CANARY}));

            MappedByteBuffer buffer = channel.map(
                FileChannel.MapMode.READ_WRITE,
                0,
                BYTES_TO_WRITE
            );
            buffer.put(data);
            // for(int i =0;i<BYTES_TO_WRITE;i++){
            //     buffer.put(CANARY);
            // }
            // buffer.force();
        }
        long endTime = System.nanoTime();
        long  elapsedMs = (endTime- startTime)/1_000_000;

        System.out.println("MappedByteBuffer write with bulk data  — " + BYTES_TO_WRITE + " bytes");
        System.out.println("Elapsed: " + elapsedMs + " ms");
        double throughputMB = (double) BYTES_TO_WRITE / 1024 / 1024;
        double throughputPerSec = throughputMB / (elapsedMs / 1000.0);
        System.out.printf("Throughput: %.2f MB in %d ms (%.2f MB/s)%n",throughputMB, elapsedMs, throughputPerSec);

    }
}
