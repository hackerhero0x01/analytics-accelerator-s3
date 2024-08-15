package com.amazon.connector.s3.common.telemetry;

import lombok.AllArgsConstructor;
import lombok.Getter;

/** Common telemetry attributes. */
@Getter
@AllArgsConstructor
public enum CommonAttributes {
  THREAD_ID("thread_id");
  private final String name;

  /**
   * Creates a and {@link Attribute} for a {@link CommonAttributes#THREAD_ID}.
   *
   * @param thread the {@link CommonAttributes#THREAD_ID} to create the attribute from.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute threadId(Thread thread) {
    return Attribute.of(CommonAttributes.THREAD_ID.getName(), thread.getId());
  }
}
