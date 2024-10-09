package com.amazon.connector.s3.access;

import lombok.AllArgsConstructor;
import lombok.Getter;

/** Kind of the stream we read from S3 */
@AllArgsConstructor
@Getter
public enum S3InputStreamKind {
  // SDK backed
  S3_SDK_GET("SDK"),
  // Ours
  S3_DAT_GET("DAT");
  private final String value;
}
