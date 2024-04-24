package com.amazon.connector.s3.object;

import lombok.Builder;
import lombok.Data;

/** Wrapper class around HeadObjectResponse abstracting away from S3-specific details */
@Data
@Builder
public class ObjectMetadata {
  long contentLength;
}
