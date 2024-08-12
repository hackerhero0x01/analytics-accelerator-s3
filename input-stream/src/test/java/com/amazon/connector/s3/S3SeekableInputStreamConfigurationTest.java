package com.amazon.connector.s3;

import static org.junit.jupiter.api.Assertions.*;

import com.amazon.connector.s3.common.telemetry.TelemetryConfiguration;
import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.physical.PhysicalIOConfiguration;
import org.junit.jupiter.api.Test;

public class S3SeekableInputStreamConfigurationTest {
  @Test
  void testDefaultBuilder() {
    S3SeekableInputStreamConfiguration configuration =
        S3SeekableInputStreamConfiguration.builder().build();
    assertEquals(PhysicalIOConfiguration.DEFAULT, configuration.getPhysicalIOConfiguration());
  }

  @Test
  void testDefault() {
    assertEquals(
        S3SeekableInputStreamConfiguration.DEFAULT,
        S3SeekableInputStreamConfiguration.builder().build());
  }

  @Test
  void testNullBlockManagerConfiguration() {
    assertThrows(
        NullPointerException.class,
        () -> S3SeekableInputStreamConfiguration.builder().physicalIOConfiguration(null).build());

    assertThrows(
        NullPointerException.class,
        () -> S3SeekableInputStreamConfiguration.builder().logicalIOConfiguration(null).build());

    assertThrows(
        NullPointerException.class,
        () -> S3SeekableInputStreamConfiguration.builder().telemetryConfiguration(null).build());
  }

  @Test
  void testNonDefaults() {
    PhysicalIOConfiguration physicalIOConfiguration =
        PhysicalIOConfiguration.builder().blobStoreCapacity(60).build();
    LogicalIOConfiguration logicalIOConfiguration =
        LogicalIOConfiguration.builder().smallObjectsPrefetchingEnabled(false).build();
    TelemetryConfiguration telemetryConfiguration =
        TelemetryConfiguration.builder().enableLogging(false).build();

    S3SeekableInputStreamConfiguration configuration =
        S3SeekableInputStreamConfiguration.builder()
            .physicalIOConfiguration(physicalIOConfiguration)
            .logicalIOConfiguration(logicalIOConfiguration)
            .telemetryConfiguration(telemetryConfiguration)
            .build();
    assertSame(physicalIOConfiguration, configuration.getPhysicalIOConfiguration());
    assertSame(logicalIOConfiguration, configuration.getLogicalIOConfiguration());
    assertSame(telemetryConfiguration, configuration.getTelemetryConfiguration());
  }
}
