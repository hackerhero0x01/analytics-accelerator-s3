package com.amazon.connector.s3.common.telemetry;

import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Telemetry operation. This represents an execution of an operation. An operation is defined by
 * name and a set of attributes.
 */
// Implementation note: the builder is implemented by hand, as opposed to via Lombok to create more
// streamlined syntax for attribute specification
@Getter
@EqualsAndHashCode(callSuper = true)
public class Metric extends TelemetryDatapoint {
  /**
   * Creates a new instance of {@link Metric}.
   *
   * @param name operation name.
   * @param attributes operation attributes.
   */
  public Metric(String name, Map<String, Attribute> attributes) {
    super(name, attributes);
  }

  /**
   * Creates a builder for {@link Metric}
   *
   * @return a new instance of {@link MetricBuilder}
   */
  public static MetricBuilder builder() {
    return new MetricBuilder();
  }

  /** Builder for {@link MetricBuilder} */
  public static class MetricBuilder extends TelemetryDatapointBuilder<Metric, MetricBuilder> {

    /**
     * Builds the {@link Metric}
     *
     * @return a new instance of {@link Metric}
     */
    @Override
    protected Metric buildCore() {
      return new Metric(this.getName(), this.getAttributes());
    }
  }
}
