package com.amazon.connector.s3.io.physical.blockmanager;

import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.io.logical.FileStatus;
import com.amazon.connector.s3.io.physical.plan.Range;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;

/**
 * A block manager for a single object.
 */
public class BlockManager implements BlockManagerInterface {
  private final MultiObjectsBlockManager multiObjectsBlockManager;
  FileStatus fileStatus;

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
    this.multiObjectsBlockManager = new MultiObjectsBlockManager(objectClient, configuration);
    this.fileStatus = new FileStatus(this.multiObjectsBlockManager.getMetadata(s3URI), s3URI);
  }

  /**
   * Creates an instance of block manager.
   *
   * @param multiObjectsBlockManager the multi objects block manager to use
   * @param s3URI the location of the object
   */
  public BlockManager(
          @NonNull MultiObjectsBlockManager multiObjectsBlockManager,
          @NonNull S3URI s3URI) {
    this.multiObjectsBlockManager = multiObjectsBlockManager;
    this.fileStatus = new FileStatus(this.multiObjectsBlockManager.getMetadata(s3URI), s3URI);
  }

  public int read(long pos) throws IOException {
    return multiObjectsBlockManager.read(pos, fileStatus.getS3URI());
  }

  public int read(byte[] buffer, int offset, int len, long pos) throws IOException {
    return multiObjectsBlockManager.read(buffer, offset, len, pos, fileStatus.getS3URI());
  }

  public int readTail(byte[] buf, int off, int n) throws IOException {
    return multiObjectsBlockManager.readTail(buf, off, n, fileStatus.getS3URI());
  }

  public CompletableFuture<ObjectMetadata> getMetadata() {
    return fileStatus.getObjectMetadata();
  }

  public void queuePrefetch(List<Range> prefetchRanges, FileStatus fileStatus) {
    multiObjectsBlockManager.queuePrefetch(prefetchRanges, fileStatus.getS3URI());
  }
  @Override
  public void close() throws IOException {
    multiObjectsBlockManager.close();
  }
}
