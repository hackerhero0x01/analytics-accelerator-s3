package com.amazon.connector.s3.io.logical.parquet;

import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.request.Range;
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
   * @param s3URI the S3URI of the object to read
   * @param logicalIOConfiguration LogicalIO configuration
   * @param physicalIO PhysicalIO instance
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
    long contentLength = physicalIO.metadata().getContentLength();
    Range tailRange = ParquetUtils.getFileTailRange(logicalIOConfiguration, 0, contentLength);
    int tailLength = (int) tailRange.getLength();

    try {
      byte[] fileTail = new byte[tailLength];
      physicalIO.readTail(fileTail, 0, tailLength);
      return new FileTail(ByteBuffer.wrap(fileTail), (int) tailRange.getLength());
    } catch (Exception e) {
      LOG.error(
          "Error in reading tail for {}. Will fallback to synchronous reading for this key.",
          s3URI.getKey(),
          e);
      throw new CompletionException("Error in getting file tail", e);
    }
  }
}
