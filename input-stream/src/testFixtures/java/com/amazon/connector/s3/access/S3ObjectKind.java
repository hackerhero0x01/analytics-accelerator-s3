package com.amazon.connector.s3.access;

import lombok.AllArgsConstructor;
import lombok.Getter;

/** S3 Object kind */
@AllArgsConstructor
@Getter
public enum S3ObjectKind {
  RANDOM_SEQUENTIAL("sequential"),
  RANDOM_PARQUET("parquet");

  private final String value;
}
