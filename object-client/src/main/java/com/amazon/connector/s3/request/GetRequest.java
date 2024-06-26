package com.amazon.connector.s3.request;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

/**
 * Object representing arguments to a GetObject call. This class helps us abstract away from S3 SDK
 * constructs.
 */
@Data
@Builder
public class GetRequest {
  @NonNull String bucket;
  @NonNull String key;
  Range range;
  String referrer;
}
