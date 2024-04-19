package com.amazon.connector.s3.util;

/** Class responsible for parsing 's3://' or 's3a://'-like S3 locations into bucket and key */
public class S3URI {

  private final String bucket;
  private final String key;

  private S3URI(String bucket, String key) {
    this.bucket = bucket;
    this.key = key;
  }

  public static S3URI of(String bucket, String key) {
    return new S3URI(bucket, key);
  }

  public String bucket() {
    return this.bucket;
  }

  public String key() {
    return this.key;
  }
}
