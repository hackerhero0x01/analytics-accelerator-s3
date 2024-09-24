package com.amazon.connector.s3.common.telemetry;

import static org.junit.jupiter.api.Assertions.*;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.ZoneId;
import java.util.Locale;
import java.util.TimeZone;
import org.junit.jupiter.api.Test;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class MetricMeasurementTest {
  private static final long TEST_EPOCH_NANOS = 1722944779101123456L;

  @Test
  void testCreate() {
    Metric metric = Metric.builder().name("S3.GET").attribute("Foo", "Bar").build();
    MetricMeasurement metricMeasurement =
        MetricMeasurement.builder()
            .metric(metric)
            .epochTimestampNanos(42L)
            .value(123L)
            .kind(MetricMeasurementKind.AGGREGATE)
            .build();

    assertSame(metric, metricMeasurement.getMetric());
    assertEquals(metric, metricMeasurement.getDatapoint());
    assertEquals(42L, metricMeasurement.getEpochTimestampNanos());
    assertEquals(123L, metricMeasurement.getValue());
    assertEquals(MetricMeasurementKind.AGGREGATE, metricMeasurement.getKind());
  }

  @Test
  void testCreateDefaults() {
    Metric metric = Metric.builder().name("S3.GET").attribute("Foo", "Bar").build();
    MetricMeasurement metricMeasurement =
        MetricMeasurement.builder().metric(metric).epochTimestampNanos(42L).value(123L).build();

    assertSame(metric, metricMeasurement.getMetric());
    assertEquals(metric, metricMeasurement.getDatapoint());
    assertEquals(42L, metricMeasurement.getEpochTimestampNanos());
    assertEquals(123L, metricMeasurement.getValue());
    assertEquals(MetricMeasurementKind.RAW, metricMeasurement.getKind());
  }

  @Test
  void testInvalidArguments() {
    Metric metric = Metric.builder().name("S3.GET").attribute("Foo", "Bar").build();
    assertThrows(
        NullPointerException.class,
        () -> MetricMeasurement.builder().epochTimestampNanos(42L).value(123L).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> MetricMeasurement.builder().metric(metric).value(123L).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> MetricMeasurement.builder().metric(metric).epochTimestampNanos(42L).build());
    assertThrows(
        NullPointerException.class,
        () ->
            MetricMeasurement.builder()
                .metric(metric)
                .value(123L)
                .epochTimestampNanos(42L)
                .kind(null)
                .build());

    assertThrows(
        NullPointerException.class,
        () ->
            MetricMeasurement.builder().metric(null).value(123L).epochTimestampNanos(42L).build());
  }

  @Test
  void testEqualsAndHashcode() {
    Metric metric = Metric.builder().name("S3.GET").attribute("Foo", "Bar").build();

    MetricMeasurement metricMeasurement1 =
        MetricMeasurement.builder().metric(metric).epochTimestampNanos(42L).value(123L).build();
    MetricMeasurement metricMeasurement2 =
        MetricMeasurement.builder().metric(metric).epochTimestampNanos(42L).value(123L).build();
    MetricMeasurement metricMeasurement3 =
        MetricMeasurement.builder().metric(metric).epochTimestampNanos(1L).value(2L).build();

    assertEquals(metricMeasurement1, metricMeasurement2);
    assertNotEquals(metricMeasurement2, metricMeasurement3);
    assertEquals(metricMeasurement1.hashCode(), metricMeasurement2.hashCode());
    assertNotEquals(metricMeasurement2.hashCode(), metricMeasurement3.hashCode());
  }

  @Test
  void testFullToString() {
    EpochFormatter epochFormatter =
        new EpochFormatter(
            EpochFormatter.DEFAULT_PATTERN,
            TimeZone.getTimeZone(ZoneId.of("BST", ZoneId.SHORT_IDS)),
            Locale.ENGLISH);
    Metric metric = Metric.builder().name("S3.GET").attribute("Foo", "Bar").build();
    MetricMeasurement metricMeasurement =
        MetricMeasurement.builder()
            .metric(metric)
            .epochTimestampNanos(TEST_EPOCH_NANOS)
            .value(123L)
            .kind(MetricMeasurementKind.AGGREGATE)
            .build();
    assertEquals(
        "[2024-08-06T17:46:19.101Z] S3.GET(Foo=Bar): 123.00",
        metricMeasurement.toString(epochFormatter));
  }

  @Test
  void testToString() {
    EpochFormatter epochFormatter =
        new EpochFormatter(
            EpochFormatter.DEFAULT_PATTERN,
            TimeZone.getTimeZone(ZoneId.of("BST", ZoneId.SHORT_IDS)),
            Locale.ENGLISH);
    Metric metric = Metric.builder().name("S3.GET").attribute("Foo", "Bar").build();
    MetricMeasurement metricMeasurement =
        MetricMeasurement.builder()
            .metric(metric)
            .epochTimestampNanos(TEST_EPOCH_NANOS)
            .value(123L)
            .kind(MetricMeasurementKind.AGGREGATE)
            .build();
    assertTrue(metricMeasurement.toString(epochFormatter).contains("] S3.GET(Foo=Bar): 123.00"));
  }

  @Test
  void tesToStringNull() {
    Metric metric = Metric.builder().name("S3.GET").attribute("Foo", "Bar").build();
    MetricMeasurement metricMeasurement =
        MetricMeasurement.builder()
            .metric(metric)
            .epochTimestampNanos(TEST_EPOCH_NANOS)
            .value(123L)
            .kind(MetricMeasurementKind.AGGREGATE)
            .build();
    assertThrows(NullPointerException.class, () -> metricMeasurement.toString(null));
  }
}
