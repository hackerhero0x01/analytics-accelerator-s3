package com.amazon.connector.s3.io.logical.parquet;

import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.io.physical.plan.Range;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Supplier;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Task for reading the tail of a parquet file. */
public class ParquetReadTailTask implements Supplier<FileTail> {

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

  @Override
  public FileTail get() {
    long contentLength = physicalIO.metadata().join().getContentLength();
    Range tailRange = ParquetUtils.getFileTailRange(logicalIOConfiguration, 0, contentLength);
    try {
      byte[] fileTail = new byte[(int) tailRange.getLength()];
      physicalIO.readTail(fileTail, 0, (int) tailRange.getLength());
      return new FileTail(ByteBuffer.wrap(fileTail), (int) tailRange.getLength());
    } catch (IOException e) {
      LOG.debug("Error in getting file tail", e);
      return null;
    }
  }
}
