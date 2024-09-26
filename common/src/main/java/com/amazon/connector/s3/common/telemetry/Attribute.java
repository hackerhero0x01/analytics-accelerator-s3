package com.amazon.connector.s3.common.telemetry;

import lombok.NonNull;
import lombok.ToString;
import lombok.Value;

/**
 * Telemetry attribute. An attribute is key/value pair associated with a telemetry item, such as
 * operation.
 */
@Value(staticConstructor = "of")
@ToString(includeFieldNames = false)
public class Attribute {
  /** Attribute name. */
  @NonNull String name;

  /** Attribute value. */
  @NonNull Object value;
}
