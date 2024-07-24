package com.amazon.connector.s3.io.physical.impl;

import com.amazon.connector.s3.common.Preconditions;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.io.physical.data.BlobStore;
import com.amazon.connector.s3.io.physical.data.MetadataStore;
import com.amazon.connector.s3.io.physical.plan.IOPlan;
import com.amazon.connector.s3.io.physical.plan.IOPlanExecution;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/** A PhysicalIO frontend */
public class PhysicalIOImpl implements PhysicalIO {

  private S3URI s3URI;
  private MetadataStore metadataStore;
  private BlobStore blobStore;

  /**
   * Construct a new instance of PhysicalIOV2.
   *
   * @param s3URI the S3 URI of the object
   * @param metadataStore a metadata cache
   * @param blobStore a data cache
   */
  public PhysicalIOImpl(S3URI s3URI, MetadataStore metadataStore, BlobStore blobStore) {
    Preconditions.checkNotNull(s3URI, "`s3URI` should not be null");
    Preconditions.checkNotNull(metadataStore, "`metadataStore` should not be null");
    Preconditions.checkNotNull(blobStore, "`blobStore` should not be null");

    this.s3URI = s3URI;
    this.metadataStore = metadataStore;
    this.blobStore = blobStore;
  }

  @Override
  public CompletableFuture<ObjectMetadata> metadata() {
    return metadataStore.get(s3URI);
  }

  @Override
  public int read(long pos) throws IOException {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");
    Preconditions.checkArgument(pos < contentLength(), "`pos` must be less than content length");

    return blobStore.open(s3URI).read(pos);
  }

  @Override
  public int read(byte[] buf, int off, int len, long pos) throws IOException {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");
    Preconditions.checkArgument(pos < contentLength(), "`pos` must be less than content length");
    Preconditions.checkArgument(0 <= off, "`off` must not be negative");
    Preconditions.checkArgument(0 <= len, "`len` must not be negative");
    Preconditions.checkArgument(off < buf.length, "`off` must be less than size of buffer");

    return blobStore.open(s3URI).read(buf, off, len, pos);
  }

  @Override
  public int readTail(byte[] buf, int off, int len) throws IOException {
    Preconditions.checkArgument(0 <= len, "`len` must not be negative");

    long contentLength = metadataStore.get(s3URI).join().getContentLength();
    return blobStore.open(s3URI).read(buf, off, len, contentLength - len);
  }

  @Override
  public IOPlanExecution execute(IOPlan ioPlan) {
    return blobStore.open(s3URI).execute(ioPlan);
  }

  private long contentLength() {
    return metadata().join().getContentLength();
  }

  @Override
  public void close() throws IOException {}
}
