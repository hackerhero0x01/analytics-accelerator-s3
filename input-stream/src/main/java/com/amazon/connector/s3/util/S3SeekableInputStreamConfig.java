package com.amazon.connector.s3.util;

import lombok.Builder;
import lombok.Getter;
import software.amazon.awssdk.services.s3.S3AsyncClient;

/** Provide configuration for S3SeekableInputStream. */
@Getter
@Builder(toBuilder = true, builderClassName = "Builder")
public class S3SeekableInputStreamConfig {
  S3AsyncClient wrappedAsyncClient;
}
