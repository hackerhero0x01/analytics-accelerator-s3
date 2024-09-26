package com.amazon.connector.s3.common.telemetry;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;

/**
 * Telemetry attribute. An attribute is key/value pair associated with a telemetry item, such as
 * operation.
 */
@Getter
@EqualsAndHashCode
public class Attribute {
  /**
   * Constructs a new instance of the {@link Attribute}
   *
   * @param name attribute name
   * @param value attribute value
   * @param aggregations aggregations
   * @return a bew instance of {@link Attribute}
   */
  public static Attribute of(@NonNull String name, @NonNull Object value, String... aggregations) {
    return new Attribute(name, value, aggregations);
  }

  /**
   * Constructs a new instance of the {@link Attribute}
   *
   * @param name attribute name
   * @param value attribute value
   * @param aggregations aggregations
   */
  private Attribute(@NonNull String name, @NonNull Object value, String... aggregations) {
    this.name = name;
    this.value = value;
    if (aggregations != null && aggregations.length > 0) {
      this.aggregations = Collections.unmodifiableList(Arrays.asList(aggregations));
    } else {
      this.aggregations = Collections.emptyList();
    }
  }

  /** Attribute name. */
  @NonNull private final String name;

  /** Attribute value. */
  @NonNull private final Object value;

  /** Aggregations used to aggregate values */
  @NonNull private final List<String> aggregations;

  /**
   * String representation of the {@link Attribute}
   *
   * @return String representation of the {@link Attribute}
   */
  public String toString() {
    if (this.aggregations.isEmpty()) {
      return "Attribute(" + this.getName() + ", " + this.getValue() + ")";
    } else {
      return "Attribute("
          + this.getName()
          + ", "
          + this.getValue()
          + "); aggregations: ["
          + String.join(", ", this.aggregations)
          + "]";
    }
  }
}
