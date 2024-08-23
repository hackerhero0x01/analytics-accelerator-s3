package com.amazon.connector.s3.io.physical.data;

import com.amazon.connector.s3.io.physical.PhysicalIOConfiguration;
import lombok.Getter;
import lombok.NonNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Responsible for tracking total memory used by an instance of {@link
 * com.amazon.connector.s3.S3SeekableInputStreamFactory}.
 */
public class MemoryTracker {

  private static final Logger LOG = LogManager.getLogger(MemoryTracker.class);

  @Getter long memoryUsed;
  long maxMemoryAllowed;

  /**
   * Constructs an instance of MemoryTracker.
   *
   * @param configuration physicalIO configuration
   */
  public MemoryTracker(@NonNull PhysicalIOConfiguration configuration) {
    this.maxMemoryAllowed = configuration.getMaxMemoryLimitBytes();
  }

  /**
   * Increment memory used.
   *
   * @param bytesAllocated bytes allocated
   * @return total memory in use
   */
  public long incrementMemoryUsed(long bytesAllocated) {
    LOG.info("MEMORY ALLOCATED {}", bytesAllocated);
    this.memoryUsed += bytesAllocated;
    return memoryUsed;
  }

  /**
   * Updates memory in use when it is freed.
   *
   * @param bytesFreed bytes freed
   * @return total memory in ues
   */
  public long freeMemory(long bytesFreed) {
    LOG.info("MEMORY FREED {}", bytesFreed);
    this.memoryUsed -= bytesFreed;
    return memoryUsed;
  }

  /**
   * Gets available memory.
   *
   * @return available memory
   */
  public long getAvailableMemory() {
    return maxMemoryAllowed - memoryUsed;
  }
}
