package com.minibroker.log;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

public class Segment {
    private final Path path;
    private final FileChannel channel;
    private final MappedByteBuffer buffer;
    private final long baseOffset;
    private final long maxSizeBytes;
}
