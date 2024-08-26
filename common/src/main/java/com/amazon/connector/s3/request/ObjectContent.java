package com.amazon.connector.s3.request;

import java.io.InputStream;
import lombok.Builder;
import lombok.Data;

/** Wrapper class around GetObjectResponse abstracting away from S3-specific details */
@Data
@Builder
public class ObjectContent {
  InputStream stream;
}
