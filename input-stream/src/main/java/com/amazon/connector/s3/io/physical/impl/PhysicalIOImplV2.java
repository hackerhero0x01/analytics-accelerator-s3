package com.amazon.connector.s3.io.physical.impl;

import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.io.physical.plan.IOPlan;
import com.amazon.connector.s3.io.physical.plan.IOPlanExecution;
import com.amazon.connector.s3.io.physical.v2.data.BlobStore;
import com.amazon.connector.s3.io.physical.v2.data.MetadataStore;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/** A PhysicalIO frontend */
public class PhysicalIOImplV2 implements PhysicalIO {

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
  public PhysicalIOImplV2(S3URI s3URI, MetadataStore metadataStore, BlobStore blobStore) {
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
    return blobStore.open(s3URI).read(pos);
  }

  @Override
  public int read(byte[] buf, int off, int len, long pos) throws IOException {
    return blobStore.open(s3URI).read(buf, off, len, pos);
  }

  @Override
  public int readTail(byte[] buf, int off, int len) throws IOException {
    long contentLength = metadataStore.get(s3URI).join().getContentLength();
    return blobStore.open(s3URI).read(buf, off, len, contentLength - len);
  }

  @Override
  public IOPlanExecution execute(IOPlan ioPlan) throws IOException {
    return blobStore.open(s3URI).execute(ioPlan);
  }

  @Override
  public void close() throws IOException {}
}
