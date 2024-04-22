package com.amazon.connector.s3.util;

import com.google.common.base.Preconditions;
import lombok.Data;

/** Class responsible for parsing 's3://' or 's3a://'-like S3 locations into bucket and key */
@Data
public class S3URI {

  private final String bucket;
  private final String key;

  private S3URI(String bucket, String key) {
    this.bucket = bucket;
    this.key = key;
  }

  public static S3URI of(String bucket, String key) {
    Preconditions.checkNotNull(bucket, "bucket must be non-null");
    Preconditions.checkNotNull(bucket, "key must be non-null");

    return new S3URI(bucket, key);
  }
}
