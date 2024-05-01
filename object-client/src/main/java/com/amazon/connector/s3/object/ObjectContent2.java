package com.amazon.connector.s3.object;

import lombok.Builder;
import lombok.Data;
import software.amazon.awssdk.core.async.SdkPublisher;

/** Wrapper class around GetObjectResponse abstracting away from S3-specific details */
@Data
@Builder
public class ObjectContent2 {
    SdkPublisher publisher;
}
