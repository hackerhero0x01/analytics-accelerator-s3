package com.amazon.connector.s3.request;

import lombok.Builder;
import lombok.Data;

/**
 * Object representing a byte range. This class helps us abstract away from S3 SDK constructs and
 * helps with testing. For example, it gets rid of the need of having to generate and parse strings
 * like "bytes=0-555" -- this is SDK detail we should not care about in layers above Object Client.
 */
@Data
@Builder
public class Range {
  long start;
  long end;
}
