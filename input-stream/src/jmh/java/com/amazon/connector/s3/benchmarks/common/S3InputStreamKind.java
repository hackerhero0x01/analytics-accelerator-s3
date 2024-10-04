package com.amazon.connector.s3.benchmarks.common;

import lombok.AllArgsConstructor;
import lombok.Getter;

/** Kind of the stream we load from S3 */
@AllArgsConstructor
@Getter
public enum S3InputStreamKind {
  S3_SDK_GET("SDK"),
  S3_DAT_GET("DAT");
  private final String value;
}
