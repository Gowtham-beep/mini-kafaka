package com.minibroker.log;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

public class SegmentedLog {
    private final Path baseDir; 
    private final List<Segment> segments;
    private volatile Segment currentSegment;
    private final ReentrantLock reentrantLock;

    public SegmentedLog(long initialOffset, Path baseDir) throws IOException{
        this.baseDir = baseDir;
        this.segments = new CopyOnWriteArrayList<>();
        this.reentrantLock = new ReentrantLock(false);

        if(!Files.exists(baseDir)){
            Files.createDirectories(baseDir);
        }

        this.currentSegment = createNewSegment(initialOffset);
        this.segments.add(currentSegment);

    }
    
    private Segment createNewSegment(long baseOffset) throws IOException{
        String baseName = String.format("%020d", baseOffset);
        Path logPath = baseDir.resolve(baseName + ".log");
        Path indexPath = baseDir.resolve(baseName + ".index");

        return new Segment(baseOffset, logPath, indexPath);

    }


}
