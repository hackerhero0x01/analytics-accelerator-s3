package com.amazon.connector.s3.request;

import com.amazon.connector.s3.util.S3URI;
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
  @NonNull S3URI s3Uri;
}
