package com.amazon.connector.s3.request;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

/**
 * Object representing arguments to a HeadObject call. This class helps us abstract away from S3 SDK
 * constructs.
 */
@Data
@Builder
public class HeadRequest {
  @NonNull String bucket;
  @NonNull String key;
}
