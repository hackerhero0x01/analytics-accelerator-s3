package com.amazon.connector.s3.io.physical.impl;

import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.common.Preconditions;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.io.physical.blockmanager.BlockManager;
import com.amazon.connector.s3.io.physical.blockmanager.BlockManagerConfiguration;
import com.amazon.connector.s3.io.physical.blockmanager.BlockManagerInterface;
import com.amazon.connector.s3.io.physical.plan.IOPlan;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.concurrent.CompletableFuture;

/** An implementation of a physical IO layer. */
public class PhysicalIOImpl implements PhysicalIO {
  private final BlockManagerInterface blockManager;

  /**
   * Construct a PhysicalIOImpl for tests.
   *
   * @param objectClient to use for physical reads
   * @param s3URI the S3 location of the object
   * @param blockManagerConfiguration configuration to use with Block Manager under the hood
   */
  protected PhysicalIOImpl(
      ObjectClient objectClient, S3URI s3URI, BlockManagerConfiguration blockManagerConfiguration) {
    Preconditions.checkNotNull(objectClient, "objectClient should not be null");
    Preconditions.checkNotNull(s3URI, "s3URI should not be null");
    Preconditions.checkNotNull(blockManagerConfiguration, "blockManagerConfiguration should not be null");

    this.blockManager = new BlockManager(objectClient, s3URI, blockManagerConfiguration);
  }

  /**
   * Construct a PhysicalIOImpl.
   *
   * @param blockManager to use
   */
  public PhysicalIOImpl(BlockManagerInterface blockManager) {
    Preconditions.checkNotNull(blockManager, "BlockManager should not be null");
    this.blockManager = blockManager;
  }

  @Override
  public void execute(IOPlan logicalIOPlan) throws InvalidParameterException {
      if  (logicalIOPlan.getPrefetchRanges() == null)
        throw new InvalidParameterException("logicalIOPlan doesn't provide information about file to read");

      this.blockManager.queuePrefetch(logicalIOPlan.getPrefetchRanges(), logicalIOPlan.getFileStatus());
  }

  @Override
  public CompletableFuture<ObjectMetadata> metadata() {
    return this.blockManager.getMetadata();
  }

  @Override
  public int read(long position) throws IOException {
    return this.blockManager.read(position);
  }

  @Override
  public int read(byte[] buf, int off, int len, long position) throws IOException {
    return this.blockManager.read(buf, off, len, position);
  }

  @Override
  public int readTail(byte[] buf, int off, int len) throws IOException {
    return this.blockManager.readTail(buf, off, len);
  }

  @Override
  public void close() throws IOException {
    try {
      this.blockManager.close();
    } catch(Exception e){
      throw new IOException(e);
    }
  }
}
