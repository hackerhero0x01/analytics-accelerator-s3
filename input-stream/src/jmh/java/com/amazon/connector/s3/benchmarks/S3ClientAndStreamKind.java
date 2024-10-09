package com.amazon.connector.s3.benchmarks;

import com.amazon.connector.s3.access.S3ClientKind;
import com.amazon.connector.s3.access.S3InputStreamKind;
import lombok.AllArgsConstructor;
import lombok.Getter;

/** Client and stream kind combined. This simplifies benchmarking parameterization and reports */
@AllArgsConstructor
@Getter
public enum S3ClientAndStreamKind {
  SDK_ASYNC_JAVA(S3ClientKind.SDK_V2_JAVA_ASYNC, S3InputStreamKind.S3_SDK_GET),
  SDK_DAT_JAVA(S3ClientKind.SDK_V2_JAVA_ASYNC, S3InputStreamKind.S3_DAT_GET),
  SDK_ASYNC_CRT(S3ClientKind.SDK_V2_CRT_ASYNC, S3InputStreamKind.S3_SDK_GET),
  SDK_DAT_CRT(S3ClientKind.SDK_V2_CRT_ASYNC, S3InputStreamKind.S3_DAT_GET);

  private final S3ClientKind clientKind;
  private final S3InputStreamKind inputStreamKind;
}
