package com.amazon.connector.s3.io.physical.v2.data;

import com.amazon.connector.s3.io.physical.plan.IOPlan;
import com.amazon.connector.s3.io.physical.plan.IOPlanExecution;
import com.amazon.connector.s3.io.physical.plan.IOPlanState;
import com.amazon.connector.s3.util.S3URI;
import java.io.Closeable;
import java.util.OptionalInt;

/** A Blob representing an object. */
public class Blob implements Closeable {

  private final S3URI s3URI;
  private final BlockManagerV2 blockManager;
  private final MetadataStore metadataStore;

  /**
   * Construct a new Blob.
   *
   * @param s3URI the S3 URI of the object
   * @param metadataStore the MetadataStore in the stream
   * @param blockManagerV2 the BlockManager for this object
   */
  public Blob(S3URI s3URI, MetadataStore metadataStore, BlockManagerV2 blockManagerV2) {
    this.s3URI = s3URI;
    this.metadataStore = metadataStore;
    this.blockManager = blockManagerV2;
  }

  /**
   * Reads a byte from the underlying object
   *
   * @param pos The position to read
   * @return an unsigned int representing the byte that was read
   */
  public int read(long pos) {
    blockManager.makePositionAvailable(pos);
    return blockManager.getBlock(pos).get().read(pos);
  }

  /**
   * Reads request data into the provided buffer
   *
   * @param buf buffer to read data into
   * @param off start position in buffer at which data is written
   * @param len length of data to be read
   * @param pos the position to begin reading from
   * @return the total number of bytes read into the buffer
   */
  public int read(byte[] buf, int off, int len, long pos) {
    blockManager.makeRangeAvailable(pos, len);

    long nextPosition = pos;
    OptionalInt numBytesRead = OptionalInt.empty();

    while (numBytesRead.orElse(0) < len && nextPosition < contentLength()) {
      Block nextBlock =
          blockManager
              .getBlock(nextPosition)
              .orElseThrow(
                  () -> new IllegalStateException("This block should have been available."));

      int bytesRead =
          nextBlock.read(
              buf, off + numBytesRead.orElse(0), len - numBytesRead.orElse(0), nextPosition);

      if (bytesRead == -1) {
        return numBytesRead.orElse(-1);
      }

      numBytesRead = OptionalInt.of(numBytesRead.orElse(0) + bytesRead);
      nextPosition += bytesRead;
    }

    return numBytesRead.orElse(0);
  }

  /**
   * Execute an IOPlan.
   *
   * @param plan the IOPlan to execute
   * @return the status of execution
   */
  public IOPlanExecution execute(IOPlan plan) {
    plan.getPrefetchRanges()
        .forEach(
            range -> {
              this.blockManager.makeRangeAvailable(range.getStart(), range.getLength());
            });

    return IOPlanExecution.builder().state(IOPlanState.SUBMITTED).build();
  }

  private long contentLength() {
    return metadataStore.get(s3URI).join().getContentLength();
  }

  @Override
  public void close() {
    this.blockManager.close();
  }
}
