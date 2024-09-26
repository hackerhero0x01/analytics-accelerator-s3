package com.amazon.connector.s3.common.telemetry;

import static org.junit.jupiter.api.Assertions.*;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.jupiter.api.Test;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class MetricTest {
  @Test
  void testCreateMetric() {
    Metric metric = Metric.builder().name("S3.GET").attribute("Foo", "Bar").build();
    assertEquals("S3.GET", metric.getName());
    assertEquals(1, metric.getAttributes().size());
    assertEquals("Bar", metric.getAttributes().get("Foo").getValue());

    // Assert immutability
    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          metric.getAttributes().put("Foo", Attribute.of("Foo", "Bar"));
        });
    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          metric.getAttributes().clear();
        });
  }

  @Test
  void testCreateMetricBuilderWithNulls() {
    assertThrows(
        NullPointerException.class,
        () -> Metric.builder().name(null).attribute("Foo", "Bar").build());
    assertThrows(
        NullPointerException.class,
        () -> Metric.builder().name("S3.GET").attribute(null, "Bar").build());
    assertThrows(
        NullPointerException.class,
        () -> Metric.builder().name("S3.GET").attribute("Foo", null).build());
    assertThrows(
        NullPointerException.class, () -> Metric.builder().name("S3.GET").attribute(null).build());
  }

  @Test
  void testCreateMetricWithAttributes() {
    Metric metric =
        Metric.builder()
            .name("S3.GET")
            .attribute(Attribute.of("s3.bucket", "bucket"))
            .attribute(Attribute.of("s3.key", "key"))
            .build();
    assertEquals("S3.GET", metric.getName());

    assertEquals(2, metric.getAttributes().size());
    assertEquals("bucket", metric.getAttributes().get("s3.bucket").getValue());
    assertEquals("key", metric.getAttributes().get("s3.key").getValue());

    // Assert immutability
    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          metric.getAttributes().put("Foo", Attribute.of("Foo", "Bar"));
        });
    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          metric.getAttributes().clear();
        });
  }

  @Test
  void testEqualsAndHashcode() {
    Metric metric1 = Metric.builder().name("S3.GET").attribute("foo", "bar").build();
    Metric metric2 = Metric.builder().name("S3.GET").attribute("foo", "bar").build();
    Metric metric3 = Metric.builder().name("S3.GET").attribute("foo1", "bar1").build();
    assertEquals(metric1, metric2);
    assertEquals(metric1.hashCode(), metric2.hashCode());

    assertNotEquals(metric3, metric2);
    assertNotEquals(metric3.hashCode(), metric2.hashCode());
  }

  @Test
  void testCreateMetricWithDuplicateAttributes() {
    Metric.MetricBuilder builder = Metric.builder().name("S3.GET").attribute("foo", "bar");
    assertThrows(IllegalArgumentException.class, () -> builder.attribute("foo", "bar"));
  }

  @Test
  void testToString() {
    assertEquals(
        "S3.GET(Foo=Bar)",
        Metric.builder().name("S3.GET").attribute("Foo", "Bar").build().toString());
    assertEquals(
        "S3.GET(Foo=Bar, X=Y)",
        Metric.builder()
            .name("S3.GET")
            .attribute("Foo", "Bar")
            .attribute("X", "Y")
            .build()
            .toString());

    assertEquals("S3.GET", Metric.builder().name("S3.GET").build().toString());
  }
}
