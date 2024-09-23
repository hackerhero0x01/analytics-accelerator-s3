package com.amazon.connector.s3;

import com.amazon.connector.s3.common.telemetry.Attribute;
import com.amazon.connector.s3.util.S3URI;
import lombok.AllArgsConstructor;
import lombok.Getter;

/** Helper class to streamline Telemetry calls. */
@Getter
@AllArgsConstructor
enum ObjectClientTelemetry {
  URI("uri"),
  RANGE("range");
  private final String name;

  public static final String OPERATION_GET = "s3.client.get";
  public static final String OPERATION_HEAD = "s3.client.head";

  /**
   * Creates an {@link Attribute} for a {@link S3URI}.
   *
   * @param s3URI the {@link S3URI} to create the attribute from.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute uri(S3URI s3URI) {
    return Attribute.of(ObjectClientTelemetry.URI.getName(), s3URI.toString());
  }
}
