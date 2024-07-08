package com.amazon.connector.s3.io.logical.parquet;

import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.io.physical.plan.Range;
import com.amazon.connector.s3.util.S3URI;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletionException;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Task for reading the tail of a parquet file. */
public class ParquetReadTailTask {

  private final S3URI s3URI;
  private final LogicalIOConfiguration logicalIOConfiguration;
  private final PhysicalIO physicalIO;

  /**
   * Creates a new instance of {@link ParquetReadTailTask}.
   *
   * @param logicalIOConfiguration logical io configuration
   * @param physicalIO physicalIO instance
   */
  public ParquetReadTailTask(
      @NonNull S3URI s3URI,
      @NonNull LogicalIOConfiguration logicalIOConfiguration,
      @NonNull PhysicalIO physicalIO) {
    this.s3URI = s3URI;
    this.logicalIOConfiguration = logicalIOConfiguration;
    this.physicalIO = physicalIO;
  }

  private static final Logger LOG = LoggerFactory.getLogger(ParquetReadTailTask.class);

  /**
   * Reads parquet file tail
   *
   * @return tail of parquet file
   */
  public FileTail readFileTail() {
    long contentLength = physicalIO.metadata().join().getContentLength();
    Range tailRange = ParquetUtils.getFileTailRange(logicalIOConfiguration, 0, contentLength);
    int tailLength = (int) tailRange.getLength() + 1;

    try {
      byte[] fileTail = new byte[tailLength];
      physicalIO.readTail(fileTail, 0, tailLength);
      return new FileTail(ByteBuffer.wrap(fileTail), tailLength);
    } catch (Exception e) {
      LOG.error(
          "Error in reading tail for {}. Will fallback to synchronous reading for this key.",
          s3URI.getKey(),
          e);
      throw new CompletionException("Error in getting file tail", e);
    }
  }
}
