package com.amazon.connector.s3.io.physical.blockmanager;

import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.common.Preconditions;
import com.amazon.connector.s3.object.ObjectContent;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.request.GetRequest;
import com.amazon.connector.s3.request.HeadRequest;
import com.amazon.connector.s3.request.Range;
import com.amazon.connector.s3.util.S3URI;
import lombok.NonNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * A multi objects block manager in charge of fetching bytes from an object store for multiple objects.
 * Currently: - Multy Objects Block Manager for multiple objects fetches bytes in 8MB chunks by default - IO blocks
 * are fixed in size (at most 8MB) and do not grow beyond their original size - Multi Objects Block Manager keeps for
 * each object the last 10 blocks alive in memory -- technically speaking this is caching, but we should be able to
 * naturally extend this logic into prefetching. - If an 11th chunk is requested, then the oldest chunk is released
 * along with all the resources it is holding.
 */
public class MultiObjectsBlockManager implements AutoCloseable {
    private final LinkedHashMap<S3URI, CompletableFuture<ObjectMetadata> > metadata;
    private final LinkedHashMap<S3URI, AutoClosingCircularBuffer<IOBlock> > ioBlocks;
    private final ObjectClient objectClient;
    private final BlockManagerConfiguration configuration;

    /**
     * Creates an instance of block manager.
     *
     * @param objectClient the Object Client to use to fetch the data
     * @param configuration configuration
     */
    public MultiObjectsBlockManager(
            @NonNull ObjectClient objectClient,
            @NonNull BlockManagerConfiguration configuration) {
        this.objectClient = objectClient;
        this.configuration = configuration;

        this.ioBlocks = new LinkedHashMap<S3URI, AutoClosingCircularBuffer<IOBlock>>(){
            @Override
            protected boolean removeEldestEntry(final Map.Entry eldest) {
                return this.size() > configuration.getCapacityMultiObjects();
            }
        };

        this.metadata = new LinkedHashMap<S3URI, CompletableFuture<ObjectMetadata>>(){
            @Override
            protected boolean removeEldestEntry(final Map.Entry eldest) {
                return this.size() > configuration.getCapacityMultiObjects();
            }
        };
    }

    public CompletableFuture<ObjectMetadata> getMetadata(S3URI s3URI) {
        if (!metadata.containsKey(s3URI)) {
            metadata.put(s3URI, objectClient.headObject(
                    HeadRequest.builder().bucket(s3URI.getBucket()).key(s3URI.getKey()).build()));
        }
        return metadata.get(s3URI);
    }

    /**
     * Reads a byte from the underlying object
     *
     * @param pos The position to read
     * @return an unsigned int representing the byte that was read
     */
    public int read(long pos, S3URI s3URI) throws IOException {
        return getBlockForPosition(pos, s3URI).getByte(pos);
    }

    /**
     * Reads request data into the provided buffer
     *
     * @param buffer buffer to read data into
     * @param offset start position in buffer at which data is written
     * @param len length of data to be read
     * @param pos the position to begin reading from
     * @return the total number of bytes read into the buffer
     */
    public int read(byte[] buffer, int offset, int len, long pos, S3URI s3URI) throws IOException {

        int numBytesRead = 0;
        int numBytesRemaining = len;
        long nextReadPos = pos;
        int nextReadOffset = offset;

        while (numBytesRemaining > 0) {

            // Reached EOF
            if (nextReadPos > getLastObjectByte(s3URI)) {
                return numBytesRead;
            }

            IOBlock ioBlock = getBlockForPosition(nextReadPos, len, s3URI);

            ioBlock.setPositionInBuffer(nextReadPos);
            ByteBuffer blockData = ioBlock.getBlockContent();

            // TODO: https://app.asana.com/0/1206885953994785/1207272185469589 - This logic can be moved
            // down to IOBlock.
            int numBytesToRead = Math.min(blockData.remaining(), numBytesRemaining);
            blockData.get(buffer, nextReadOffset, numBytesToRead);
            nextReadOffset += numBytesToRead;
            nextReadPos += numBytesToRead;
            numBytesRemaining -= numBytesToRead;
            numBytesRead += numBytesToRead;
        }

        return numBytesRead;
    }

    /**
     * Reads the last n bytes from the object.
     *
     * @param buf byte buffer to read into
     * @param off position of first read byte in the byte buffer
     * @param n length of data to read in bytes
     * @return the number of bytes read or -1 when EOF is reached
     */
    public int readTail(byte[] buf, int off, int n, S3URI s3URI) throws IOException {
        Preconditions.checkArgument(0 <= n, "must request a non-negative number of bytes from tail");
        Preconditions.checkArgument(
                n <= contentLength(s3URI), "cannot request more bytes from tail than total number of bytes");

        long start = contentLength(s3URI) - n;
        return read(buf, off, n, start, s3URI);
    }

    private IOBlock getBlockForPosition(long pos, int len, S3URI s3URI) throws IOException {
        Optional<IOBlock> lookup = lookupBlockForPosition(pos, s3URI);
        if (!lookup.isPresent()) {
            return createBlockStartingAtWithSize(pos, len, s3URI);
        }

        return lookup.get();
    }

    private IOBlock getBlockForPosition(long pos, S3URI s3URI) throws IOException {
        Optional<IOBlock> lookup = lookupBlockForPosition(pos, s3URI);
        if (!lookup.isPresent()) {
            return createBlockStartingAt(pos, s3URI);
        }

        return lookup.get();
    }

    private Optional<IOBlock> lookupBlockForPosition(long pos, S3URI s3URI) {
        AutoClosingCircularBuffer<IOBlock> blocks =
                ioBlocks.computeIfAbsent(s3URI,
                                         block -> new AutoClosingCircularBuffer<>(configuration.getCapacityMultiObjects()));
        return blocks.stream().filter(block -> block.contains(pos)).findFirst();
    }

    private IOBlock createBlockStartingAt(long start, S3URI s3URI) throws IOException {
        long end = Math.min(start + configuration.getBlockSizeBytes() - 1, getLastObjectByte(s3URI));

        return createBlock(start, end, s3URI);
    }

    private IOBlock createBlockStartingAtWithSize(long start, int size, S3URI s3URI) throws IOException {
        long end;

        if (size > configuration.getReadAheadBytes()) {
            end = Math.min(start + size - 1, getLastObjectByte(s3URI));
        } else {
            end = Math.min(start + configuration.getReadAheadBytes() - 1, getLastObjectByte(s3URI));
        }

        return createBlock(start, end, s3URI);
    }

    private IOBlock createBlock(long start, long end, S3URI s3URI) throws IOException {
        CompletableFuture<ObjectContent> objectContent =
                this.objectClient.getObject(
                        GetRequest.builder()
                                .bucket(s3URI.getBucket())
                                .key(s3URI.getKey())
                                .range(Range.builder().start(start).end(end).build())
                                .build());

        IOBlock ioBlock = new IOBlock(start, end, objectContent);
        AutoClosingCircularBuffer<IOBlock> blocks =
                ioBlocks.computeIfAbsent(s3URI,
                                         block -> new AutoClosingCircularBuffer<>(configuration.getCapacityMultiObjects()));
        blocks.add(ioBlock);
        return ioBlock;
    }

    private long contentLength(S3URI s3URI) {
        return this.getMetadata(s3URI).join().getContentLength();
    }

    private long getLastObjectByte(S3URI s3URI) {
        return contentLength(s3URI) - 1;
    }

    @Override
    public void close() throws IOException {
        this.ioBlocks.forEach((key, block) -> {
            try {
                block.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        this.ioBlocks.clear();
    }

    public void queuePrefetch(final List<com.amazon.connector.s3.io.physical.plan.Range> prefetchRanges,
                              final S3URI s3URI) {
        this.ioBlocks.computeIfAbsent(s3URI,
                                      block -> new AutoClosingCircularBuffer<>(configuration.getCapacityMultiObjects()));
        prefetchRanges.forEach(range -> {
            try {
                createBlock(range.getStart(), range.getEnd(), s3URI);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
