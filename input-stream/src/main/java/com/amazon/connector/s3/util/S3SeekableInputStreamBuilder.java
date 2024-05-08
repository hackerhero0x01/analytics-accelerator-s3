package com.amazon.connector.s3.util;

import lombok.Builder;
import lombok.Getter;
import software.amazon.awssdk.services.s3.S3AsyncClient;

/** Builder for S3SeekableInputStream. */
@Builder
public class S3SeekableInputStreamBuilder {
  @Getter S3AsyncClient wrappedAsyncClient;
}
