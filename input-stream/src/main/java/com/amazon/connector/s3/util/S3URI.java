package com.amazon.connector.s3.util;

import com.amazon.connector.s3.common.Preconditions;
import lombok.Data;

/** Container for representing an 's3://' or 's3a://'-style S3 location. */
@Data
public class S3URI {

  private final String bucket;
  private final String key;

  private S3URI(String bucket, String key) {
    this.bucket = bucket;
    this.key = key;
  }

  /** Given a bucket and a key, creates an S3URI object. */
  public static S3URI of(String bucket, String key) {
    Preconditions.checkNotNull(bucket, "bucket must be non-null");
    Preconditions.checkNotNull(key, "key must be non-null");

    return new S3URI(bucket, key);
  }
}
