package com.amazon.connector.s3.request;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/**
 * Object representing arguments to a GetObject call. This class helps us abstract away from S3 SDK
 * constructs.
 */
@Value
@Builder
public class GetRequest {
  @NonNull String bucket;
  @NonNull String key;
  @NonNull Range range;
  @NonNull Referrer referrer;
}
