package com.amazon.connector.s3.io.physical.blockmanager;

import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.io.logical.ObjectStatus;
import com.amazon.connector.s3.io.physical.plan.Range;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;

/** A block manager for a single object. */
public class BlockManager implements BlockManagerInterface {
  private final MultiObjectsBlockManager multiObjectsBlockManager;
  private final ObjectStatus objectStatus;
  private boolean ownsMultiObjectsBlockManager = false;
  private final S3URI s3URI;

  /**
   * Creates an instance of block manager.
   *
   * @param objectClient the Object Client to use to fetch the data
   * @param s3URI the location of the object
   * @param configuration configuration
   */
  public BlockManager(
      @NonNull ObjectClient objectClient,
      @NonNull S3URI s3URI,
      @NonNull BlockManagerConfiguration configuration) {
    this.ownsMultiObjectsBlockManager = true;
    this.multiObjectsBlockManager = new MultiObjectsBlockManager(objectClient, configuration);
    this.objectStatus = new ObjectStatus(this.multiObjectsBlockManager.getMetadata(s3URI), s3URI);
    this.s3URI = s3URI;
  }

  /**
   * Creates an instance of block manager.
   *
   * @param multiObjectsBlockManager the multi objects block manager to use
   * @param s3URI the location of the object
   */
  public BlockManager(
      @NonNull MultiObjectsBlockManager multiObjectsBlockManager, @NonNull S3URI s3URI) {
    this.multiObjectsBlockManager = multiObjectsBlockManager;
    this.objectStatus = new ObjectStatus(this.multiObjectsBlockManager.getMetadata(s3URI), s3URI);
    this.s3URI = s3URI;
  }

  @Override
  public int read(long pos) throws IOException {
    return multiObjectsBlockManager.read(pos, objectStatus.getS3URI());
  }

  @Override
  public int read(byte[] buffer, int offset, int len, long pos) throws IOException {
    return multiObjectsBlockManager.read(buffer, offset, len, pos, objectStatus.getS3URI());
  }

  @Override
  public int readTail(byte[] buf, int off, int n) throws IOException {
    return multiObjectsBlockManager.readTail(buf, off, n, objectStatus.getS3URI());
  }

  @Override
  public CompletableFuture<ObjectMetadata> getMetadata() {
    return objectStatus.getObjectMetadata();
  }

  public void queuePrefetch(List<Range> prefetchRanges) {
    multiObjectsBlockManager.queuePrefetch(prefetchRanges, objectStatus.getS3URI());
  }

  @Override
  public void close() throws IOException {
    if (ownsMultiObjectsBlockManager) {
      multiObjectsBlockManager.close();
    }
  }
}
