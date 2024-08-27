package com.amazon.connector.s3.util;

import com.amazon.connector.s3.common.telemetry.Attribute;
import com.amazon.connector.s3.io.physical.plan.IOPlan;
import com.amazon.connector.s3.request.Range;
import lombok.AllArgsConstructor;
import lombok.Getter;

/** Helper class to streamline Telemetry calls. */
@Getter
@AllArgsConstructor
public enum StreamAttributes {
  URI("uri"),
  RANGE("range"),
  FLAVOR("flavor"),
  EFFECTIVE_RANGE("range.effective"),
  GENERATION("generation"),
  COLUMN("column"),
  IOPLAN("ioplan");
  private final String name;

  /**
   * Creates an {@link Attribute} for a {@link S3URI}.
   *
   * @param s3URI the {@link S3URI} to create the attribute from.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute uri(S3URI s3URI) {
    return Attribute.of(StreamAttributes.URI.getName(), s3URI.toString());
  }

  /**
   * Creates an {@link Attribute} for a {@link S3URI}.
   *
   * @param flavor the flavor of the operation
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute flavor(String flavor) {
    return Attribute.of(StreamAttributes.FLAVOR.getName(), flavor);
  }

  /**
   * Creates an {@link Attribute} for a {@link Range}.
   *
   * @param start range start.
   * @param end range end.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute range(long start, long end) {
    return range(new Range(start, end));
  }

  /**
   * Creates an {@link Attribute} for a {@link Range}.
   *
   * @param range range.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute range(Range range) {
    return Attribute.of(StreamAttributes.RANGE.getName(), range.toString());
  }

  /**
   * Creates an {@link Attribute} for anm affective {@link Range}.
   *
   * @param start range start.
   * @param end range end.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute effectiveRange(long start, long end) {
    return effectiveRange(new Range(start, end));
  }

  /**
   * Creates an {@link Attribute} for anm affective {@link Range}.
   *
   * @param range range.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute effectiveRange(Range range) {
    return Attribute.of(StreamAttributes.EFFECTIVE_RANGE.getName(), range.toString());
  }

  /**
   * Creates an {@link Attribute} for generation.
   *
   * @param generation the position to create the attribute from.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute generation(long generation) {
    return Attribute.of(StreamAttributes.GENERATION.getName(), generation);
  }

  /**
   * Creates an {@link Attribute} for generation.
   *
   * @param column the position to create the attribute from.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute column(String column) {
    return Attribute.of(StreamAttributes.COLUMN.getName(), column);
  }

  /**
   * Creates an {@link Attribute} for ioPlan.
   *
   * @param ioPlan the position to create the attribute from.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute ioPlan(IOPlan ioPlan) {
    return Attribute.of(StreamAttributes.IOPLAN.getName(), ioPlan.toString());
  }
}
