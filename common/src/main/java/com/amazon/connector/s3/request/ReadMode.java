package com.amazon.connector.s3.request;

/**
 * Enum to help with the annotation of reads. We mark reads SYNC when they were triggered by a
 * synchronous read or ASYNC when they were to do logical or physical prefetching.
 */
public enum ReadMode {
  SYNC,
  ASYNC;
}
