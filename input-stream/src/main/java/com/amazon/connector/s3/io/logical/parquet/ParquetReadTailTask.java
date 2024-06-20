package com.amazon.connector.s3.io.logical.parquet;

import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.io.physical.plan.Range;
import java.nio.ByteBuffer;
import java.util.Optional;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Task for reading the tail of a parquet file. */
public class ParquetReadTailTask {

  private final LogicalIOConfiguration logicalIOConfiguration;
  private final PhysicalIO physicalIO;

  /**
   * Creates a new instance of {@link ParquetReadTailTask}.
   *
   * @param logicalIOConfiguration logical io configuration
   * @param physicalIO physicalIO instance
   */
  public ParquetReadTailTask(
      @NonNull LogicalIOConfiguration logicalIOConfiguration, @NonNull PhysicalIO physicalIO) {
    this.logicalIOConfiguration = logicalIOConfiguration;
    this.physicalIO = physicalIO;
  }

  private static final Logger LOG = LoggerFactory.getLogger(ParquetReadTailTask.class);

  /**
   * Reads parquet file tail
   *
   * @return tail of parquet file
   */
  public Optional<FileTail> readFileTail() {
    long contentLength = physicalIO.metadata().join().getContentLength();
    Range tailRange = ParquetUtils.getFileTailRange(logicalIOConfiguration, 0, contentLength);
    int tailLength = (int) tailRange.getLength() + 1;
    try {
      byte[] fileTail = new byte[tailLength];
      physicalIO.readTail(fileTail, 0, tailLength);
      return Optional.of(new FileTail(ByteBuffer.wrap(fileTail), tailLength));
    } catch (Exception e) {
      LOG.debug("Error in getting file tail", e);
    }

    return Optional.empty();
  }
}
