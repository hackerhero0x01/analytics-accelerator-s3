package com.amazon.connector.s3.io.physical.impl;

import com.amazon.connector.s3.common.Preconditions;
import com.amazon.connector.s3.common.telemetry.Operation;
import com.amazon.connector.s3.common.telemetry.Telemetry;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.io.physical.data.BlobStore;
import com.amazon.connector.s3.io.physical.data.MetadataStore;
import com.amazon.connector.s3.io.physical.plan.IOPlan;
import com.amazon.connector.s3.io.physical.plan.IOPlanExecution;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.util.S3URI;
import com.amazon.connector.s3.util.StreamAttributes;
import java.io.IOException;
import lombok.NonNull;

/** A PhysicalIO frontend */
public class PhysicalIOImpl implements PhysicalIO {
  private final S3URI s3URI;
  private final MetadataStore metadataStore;
  private final BlobStore blobStore;
  private final Telemetry telemetry;

  private static final String OPERATION_READ_TAIL = "physical.io.read.tail";
  private static final String OPERATION_EXECUTE = "physical.io.execute";

  /**
   * Construct a new instance of PhysicalIOV2.
   *
   * @param s3URI the S3 URI of the object
   * @param metadataStore a metadata cache
   * @param blobStore a data cache
   * @param telemetry The {@link Telemetry} to use to report measurements.
   */
  public PhysicalIOImpl(
      @NonNull S3URI s3URI,
      @NonNull MetadataStore metadataStore,
      @NonNull BlobStore blobStore,
      @NonNull Telemetry telemetry) {
    this.s3URI = s3URI;
    this.metadataStore = metadataStore;
    this.blobStore = blobStore;
    this.telemetry = telemetry;
  }

  @Override
  public ObjectMetadata metadata() {
    return metadataStore.get(s3URI);
  }

  @Override
  public int read(long pos) throws IOException {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");
    Preconditions.checkArgument(pos < contentLength(), "`pos` must be less than content length");

    return blobStore.get(s3URI).read(pos);
  }

  @Override
  public int read(byte[] buf, int off, int len, long pos) throws IOException {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");
    Preconditions.checkArgument(pos < contentLength(), "`pos` must be less than content length");
    Preconditions.checkArgument(0 <= off, "`off` must not be negative");
    Preconditions.checkArgument(0 <= len, "`len` must not be negative");
    Preconditions.checkArgument(off < buf.length, "`off` must be less than size of buffer");

    return blobStore.get(s3URI).read(buf, off, len, pos);
  }

  @Override
  public int readTail(byte[] buf, int off, int len) throws IOException {
    Preconditions.checkArgument(0 <= len, "`len` must not be negative");

    return telemetry.measureStandard(
        () ->
            Operation.builder()
                .name(OPERATION_READ_TAIL)
                .attribute(StreamAttributes.uri(this.s3URI))
                .attribute(StreamAttributes.offset(off))
                .attribute(StreamAttributes.length(len))
                .build(),
        () -> {
          long contentLength = contentLength();
          return blobStore.get(s3URI).read(buf, off, len, contentLength - len);
        });
  }

  @Override
  public IOPlanExecution execute(IOPlan ioPlan) {
    return telemetry.measureStandard(
        () ->
            Operation.builder()
                .name(OPERATION_EXECUTE)
                .attribute(StreamAttributes.uri(this.s3URI))
                .attribute(StreamAttributes.ioPlan(ioPlan))
                .build(),
        () -> blobStore.get(s3URI).execute(ioPlan));
  }

  private long contentLength() {
    return metadata().getContentLength();
  }

  @Override
  public void close() throws IOException {}
}
