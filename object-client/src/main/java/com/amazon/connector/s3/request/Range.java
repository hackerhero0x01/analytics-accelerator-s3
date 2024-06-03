package com.amazon.connector.s3.request;

import com.amazon.connector.s3.common.Preconditions;
import java.util.OptionalLong;
import lombok.Getter;
import lombok.NonNull;

/**
 * Object representing a byte range. This class helps us abstract away from S3 SDK constructs and
 * helps with testing. For example, it gets rid of the need of having to generate and parse strings
 * like "bytes=0-555" -- this is SDK detail we should not care about in layers above Object Client.
 */
public class Range {
  @Getter OptionalLong start;
  @Getter OptionalLong end;

  /**
   * Construct a range. At least one of the start and end of range should be present.
   *
   * @param start the start of the range, possibly empty
   * @param end the end of the range, possibly empty
   */
  public Range(@NonNull OptionalLong start, @NonNull OptionalLong end) {
    Preconditions.checkArgument(
        start.isPresent() || end.isPresent(),
        "invalid range: at least one of start or end needs to be present");
    Preconditions.checkArgument(start.orElse(0L) >= 0, "start must not be negative");
    Preconditions.checkArgument(end.orElse(0L) >= 0, "end must not be negative");

    this.start = start;
    this.end = end;
  }

  @Override
  public String toString() {
    String s = start.isPresent() ? Long.toString(start.getAsLong()) : "";
    String e = end.isPresent() ? Long.toString(end.getAsLong()) : "";

    return "bytes=" + s + "-" + e;
  }
}
