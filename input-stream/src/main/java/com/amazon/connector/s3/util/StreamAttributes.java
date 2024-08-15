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
  POSITION("position"),
  OFFSET("offset"),
  LENGTH("length"),
  START("start"),
  END("end"),
  GENERATION("generation"),
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
   * Creates an {@link Attribute} for a {@link Range}.
   *
   * @param range the {@link Range} to create the attribute from.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute range(Range range) {
    return Attribute.of(StreamAttributes.RANGE.getName(), range.toString());
  }

  /**
   * Creates an {@link Attribute} for a position.
   *
   * @param position the position to create the attribute from.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute position(long position) {
    return Attribute.of(StreamAttributes.POSITION.getName(), position);
  }

  /**
   * Creates an {@link Attribute} for a length.
   *
   * @param length the position to create the attribute from.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute length(long length) {
    return Attribute.of(StreamAttributes.LENGTH.getName(), length);
  }

  /**
   * Creates an {@link Attribute} for a length.
   *
   * @param offset the position to create the attribute from.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute offset(long offset) {
    return Attribute.of(StreamAttributes.OFFSET.getName(), offset);
  }

  /**
   * Creates an {@link Attribute} for start.
   *
   * @param start the position to create the attribute from.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute start(long start) {
    return Attribute.of(StreamAttributes.START.getName(), start);
  }

  /**
   * Creates an {@link Attribute} for end.
   *
   * @param end the position to create the attribute from.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute end(long end) {
    return Attribute.of(StreamAttributes.END.getName(), end);
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
   * Creates an {@link Attribute} for ioPlan.
   *
   * @param ioPlan the position to create the attribute from.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute ioPlan(IOPlan ioPlan) {
    return Attribute.of(StreamAttributes.IOPLAN.getName(), ioPlan.toString());
  }
}
