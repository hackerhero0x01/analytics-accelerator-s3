package com.amazon.connector.s3.common.telemetry;

import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Telemetry metric. A metric is a template for a specific measurement and is defined by a name and
 * a set of attributes It doesn't carry the actual value - that is defined by a specific instance of
 * a metric, represented by {@link MetricMeasurement}
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
  private Metric(String name, Map<String, Attribute> attributes) {
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
