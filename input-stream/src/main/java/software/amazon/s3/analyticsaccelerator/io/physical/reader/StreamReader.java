/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package software.amazon.s3.analyticsaccelerator.io.physical.reader;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;
import software.amazon.s3.analyticsaccelerator.io.physical.data.DataBlock;
import software.amazon.s3.analyticsaccelerator.request.*;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;

/**
 * {@code StreamReader} is responsible for asynchronously reading a range of bytes from an object in
 * S3 and populating the corresponding {@link DataBlock}s with the downloaded data.
 *
 * <p>It submits the read task to a provided {@link ExecutorService}, allowing non-blocking
 * operation.
 */
@SuppressFBWarnings(
    value = "RCN_REDUNDANT_NULLCHECK",
    justification = "Returning null is intentional to signal premature EOF")
public class StreamReader implements Closeable {
  private final ObjectClient objectClient;
  private final ObjectKey objectKey;
  private final ExecutorService threadPool;
  private final Consumer<List<DataBlock>> removeBlocksFunc;
  private final OpenStreamInformation openStreamInformation;

  private static final Logger LOG = LoggerFactory.getLogger(StreamReader.class);

  /**
   * Constructs a {@code StreamReader} instance for reading objects from S3.
   *
   * @param objectClient the client used to fetch S3 object content
   * @param objectKey the key identifying the S3 object and its ETag
   * @param threadPool an {@link ExecutorService} used for async I/O operations
   * @param removeBlocksFunc a function to remove blocks from
   * @param openStreamInformation contains stream information
   */
  public StreamReader(
      @NonNull ObjectClient objectClient,
      @NonNull ObjectKey objectKey,
      @NonNull ExecutorService threadPool,
      @NonNull Consumer<List<DataBlock>> removeBlocksFunc,
      @NonNull OpenStreamInformation openStreamInformation) {
    this.objectClient = objectClient;
    this.objectKey = objectKey;
    this.threadPool = threadPool;
    this.removeBlocksFunc = removeBlocksFunc;
    this.openStreamInformation = openStreamInformation;
  }

  /**
   * Asynchronously reads a range of bytes from the S3 object and fills the corresponding {@link
   * DataBlock}s with data. The byte range is determined by the start of the first block and the end
   * of the last block.
   *
   * @param blocks the list of {@link DataBlock}s to be populated; must not be empty and must be
   *     sorted by offset
   * @param readMode the mode in which the read is being performed (used for tracking or metrics)
   * @throws IllegalArgumentException if the {@code blocks} list is empty
   * @implNote This method uses a fire-and-forget strategy and doesn't return a {@code Future};
   *     failures are logged or wrapped in a {@code IOException}.
   */
  @SuppressFBWarnings(
      value = "RV_RETURN_VALUE_IGNORED",
      justification = "Intentional fire-and-forget task")
  public void read(@NonNull final List<DataBlock> blocks, ReadMode readMode) {
    Preconditions.checkArgument(!blocks.isEmpty(), "`blocks` list must not be empty");
    threadPool.submit(processReadTask(blocks, readMode));
  }

  private Runnable processReadTask(final List<DataBlock> blocks, ReadMode readMode) {
    return () -> {
      Range requestRange = computeRange(blocks);

      GetRequest getRequest =
          GetRequest.builder()
              .s3Uri(objectKey.getS3URI())
              .range(requestRange)
              .etag(objectKey.getEtag())
              .referrer(new Referrer(requestRange.toHttpString(), readMode))
              .build();

      ObjectContent objectContent = fetchObjectContent(getRequest);

      if (objectContent == null) {
        // Couldn't successfully get the response from S3.
        // Remove blocks from store and complete async operation
        removeNonFilledBlocksFromStore(blocks);
        return;
      }

      try (InputStream inputStream = objectContent.getStream()) {
        boolean success = readBlocksFromStream(inputStream, blocks, requestRange.getStart());
        if (!success) {
          removeNonFilledBlocksFromStore(blocks);
        }
      } catch (EOFException e) {
        LOG.error("EOFException while reading blocks", e);
        removeNonFilledBlocksFromStore(blocks);
      } catch (IOException e) {
        LOG.error("IOException while reading blocks", e);
        removeNonFilledBlocksFromStore(blocks);
      }
    };
  }

  private boolean readBlocksFromStream(
      InputStream inputStream, List<DataBlock> blocks, long initialOffset) throws IOException {
    long currentOffset = initialOffset;
    for (DataBlock block : blocks) {
      boolean success = readBlock(inputStream, block, currentOffset);
      if (!success) {
        return false;
      }

      long blockSize =
          block.getBlockKey().getRange().getEnd() - block.getBlockKey().getRange().getStart() + 1;
      currentOffset += blockSize;
    }
    return true;
  }

  private Range computeRange(List<DataBlock> blocks) {
    long rangeStart = blocks.get(0).getBlockKey().getRange().getStart();
    long rangeEnd = blocks.get(blocks.size() - 1).getBlockKey().getRange().getEnd();
    return new Range(rangeStart, rangeEnd);
  }

  private ObjectContent fetchObjectContent(GetRequest getRequest) {
    try {
      return this.objectClient.getObject(getRequest, this.openStreamInformation).join();
    } catch (Exception e) {
      LOG.error("Error while fetching object content", e);
      return null;
    }
  }

  private boolean readBlock(InputStream inputStream, DataBlock block, long currentOffset)
      throws IOException {
    long blockStart = block.getBlockKey().getRange().getStart();
    long blockEnd = block.getBlockKey().getRange().getEnd();
    int blockSize = (int) (blockEnd - blockStart + 1);

    // Skip if needed
    if (!skipToBlockStart(inputStream, blockStart, currentOffset)) {
      return false;
    }

    // Read block data
    byte[] blockData = readExactBytes(inputStream, blockSize);
    if (blockData == null) {
      return false;
    }

    block.setData(blockData);
    return true;
  }

  private boolean skipToBlockStart(InputStream inputStream, long blockStart, long currentOffset)
      throws IOException {
    long skipBytes = blockStart - currentOffset;
    if (skipBytes <= 0) {
      return true;
    }

    long totalSkipped = 0;
    while (totalSkipped < skipBytes) {
      long skipped = inputStream.skip(skipBytes - totalSkipped);
      if (skipped <= 0) {
        return false;
      }
      totalSkipped += skipped;
    }

    return true;
  }

  /**
   * Attempts to read exactly {@code size} bytes from the input stream. Returns {@code null} if the
   * end of the stream is reached before reading all bytes.
   *
   * @param inputStream The input stream to read from.
   * @param size Number of bytes to read.
   * @return A byte array of exactly {@code size} bytes, or {@code null} on premature EOF.
   */
  private byte[] readExactBytes(InputStream inputStream, int size) throws IOException {
    byte[] buffer = new byte[size];
    int totalRead = 0;
    while (totalRead < size) {
      int bytesRead = inputStream.read(buffer, totalRead, size - totalRead);
      if (bytesRead == -1) {
        throw new EOFException("Premature EOF: expected " + size + " bytes, but got " + totalRead);
      }
      totalRead += bytesRead;
    }
    return buffer;
  }

  private void removeNonFilledBlocksFromStore(List<DataBlock> blocks) {
    this.removeBlocksFunc.accept(
        blocks.stream().filter(block -> !block.isDataReady()).collect(Collectors.toList()));
  }

  /**
   * Closes the underlying {@link ObjectClient} and shuts down the thread pool used for asynchronous
   * execution.
   *
   * @throws IOException if the {@code objectClient} fails to close properly
   */
  @Override
  public void close() throws IOException {
    try {
      this.objectClient.close();
    } finally {
      this.threadPool.shutdown();
    }
  }
}
