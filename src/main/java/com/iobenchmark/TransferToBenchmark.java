package com.iobenchmark;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.nio.file.Path;

public class TransferToBenchmark {
    public static final String FILE_PATH ="data/transferto_source.bin";
    public static final String DEST_FILE_PATH = "data/transferto_destination.bin";
    public static final int BYTES_TO_TRANSFER = 1_000_000;
    public static final byte CANARY = 0x42;
    public static void main(String[] args) throws IOException{
        byte[] sourceData = new byte[BYTES_TO_TRANSFER];
        for(int i =0;i<BYTES_TO_TRANSFER;i++){
            sourceData[i] = CANARY;
        }
        try(FileOutputStream fos = new FileOutputStream(FILE_PATH,false)){
            fos.write(sourceData);
        }
        long startTime = System.nanoTime();

        try(
            FileChannel source = FileChannel.open(
                Path.of(FILE_PATH),
                StandardOpenOption.READ
            );
            FileChannel destination = FileChannel.open(
                Path.of(DEST_FILE_PATH),
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
            )
        ){
            long transferred = 0;

            while(transferred<BYTES_TO_TRANSFER){
                long remaining = BYTES_TO_TRANSFER - transferred;
                long n = source.transferTo(transferred, remaining, destination);
                transferred += n;

            }
        }
        long endTime = System.nanoTime();
        long elapsedMs = (endTime - startTime) / 1_000_000;

        double throughputMB = (double) BYTES_TO_TRANSFER / 1024 / 1024;
        double throughputPerSec = elapsedMs > 0
                ? throughputMB / (elapsedMs / 1000.0)
                : Double.POSITIVE_INFINITY; // sub-millisecond — we'll handle this

        System.out.println("FileChannel.transferTo() — " + BYTES_TO_TRANSFER + " bytes");
        System.out.printf("Elapsed: %d ms%n", elapsedMs);
        System.out.printf("Throughput: %.2f MB in %d ms (%.2f MB/s)%n",
                throughputMB, elapsedMs, throughputPerSec);
    }
}
