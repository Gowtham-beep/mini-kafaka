package com.minibroker.log;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;


public class SegmentedLog {
    private final Path baseDir; 
    private final List<Segment> segments;
    private volatile Segment currentSegment;
    private final ReentrantLock reentrantLock;
    private final long maxFileSize;

    public SegmentedLog(long initialOffset, Path baseDir, long maxFileSize) throws IOException{
        this.baseDir = baseDir;
        this.segments = new CopyOnWriteArrayList<>();
        this.reentrantLock = new ReentrantLock(false);
        this.maxFileSize = maxFileSize;

        if(!Files.exists(baseDir)){
            Files.createDirectories(baseDir);
        }

        this.currentSegment = createNewSegment(initialOffset);
        this.segments.add(currentSegment);

    }

    public SegmentedLog(long initialOffset, Path baseDir) throws IOException {
        this(initialOffset, baseDir, Segment.DEFAULT_MAX_FILE_SIZE);
    }
    
    private Segment createNewSegment(long baseOffset) throws IOException{
        String baseName = String.format("%020d", baseOffset);
        Path logPath = baseDir.resolve(baseName + ".log");
        Path indexPath = baseDir.resolve(baseName + ".index");

        return new Segment(baseOffset, logPath, indexPath, maxFileSize);
    }
//hot path 
    public long append(byte[] payload){
        while(true){
            Segment activeSegment = this.currentSegment;
            try{
                return activeSegment.append(payload);

            }catch(IllegalStateException e){
                reentrantLock.lock();
                try{
                    if(this.currentSegment == activeSegment){
                        rotate(activeSegment);
                    }
                }finally{
                    reentrantLock.unlock();
                }
            }
        }
    }
//cold path
    private void rotate(Segment oldSegment){
        try{
            oldSegment.seal();
            long newBaseOffset = oldSegment.getBaseOffset() + oldSegment.getMessageCount();
            Segment newSegment = createNewSegment(newBaseOffset);
            this.segments.add(newSegment);
            this.currentSegment = newSegment;

        }catch( IOException e){
            throw new RuntimeException("CRITICAL IO ERROR: Failed to rotate segment",e);
        }
    }

    public byte[] read(long logicalOffset){
        Segment targetSegment = findSegment(logicalOffset);
        if(targetSegment == null){
            throw new IllegalArgumentException(
                "Offset" + logicalOffset + "is out of range"
            );
        }
        return targetSegment.read(logicalOffset);
    }

    private Segment findSegment(long logicalOffset){
        int lo =0;
        int hi = segments.size() -1;
        Segment floorSegment = null;

        while(lo<=hi){
            int mid = lo + (hi-lo)/2;
            Segment midSegment = segments.get(mid);
            long midBaseOffset = midSegment.getBaseOffset();
            if(midBaseOffset == logicalOffset){
                return midSegment;
            }else if (midBaseOffset< logicalOffset){
                floorSegment = midSegment;
                lo = mid + 1;
            }else{
                hi = mid -1;

            }
        }
        if(floorSegment!=null){
            long maxOffsetInSegment = floorSegment.getBaseOffset()+floorSegment.getMessageCount();
            if(logicalOffset< maxOffsetInSegment){
                return floorSegment;
            }
        }
        return null; 
    }

    public void close(){
        reentrantLock.lock();
        try {
            for(Segment segment : segments){
                if(!segment.isSealed()){
                    segment.seal();
                }else{
                    segment.closeChannels();
                }
            }
        }catch(IOException e){
            System.err.println("Error closing segment: " + e.getMessage());
        }
         finally {
            reentrantLock.unlock();
        }

    }

    public void delete(long retentionOffset){
        List<Segment> expiredSegments = new ArrayList<>();

        for(int i=0;i<segments.size()-1;i++){

            Segment curr = segments.get(i);
            Segment next = segments.get(i+1);

            if(next.getBaseOffset() <=retentionOffset){
                expiredSegments.add(curr);
            }else{
                break;
            }
        }
        if(expiredSegments.isEmpty()){
            return;
        }
        this.segments.removeAll(expiredSegments);

        for(Segment segment: expiredSegments ){
            try {
                segment.deleteFiles();
            } catch (Exception e) {
                System.err.println("Failed to delete segment: " + e.getMessage());
            }
            
        }

    }
}
