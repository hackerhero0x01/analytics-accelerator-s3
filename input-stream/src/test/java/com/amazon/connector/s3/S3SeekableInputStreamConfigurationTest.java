package com.amazon.connector.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

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
  }

  @Test
  void testNonDefaults() {
    PhysicalIOConfiguration physicalIOConfiguration = mock(PhysicalIOConfiguration.class);
    S3SeekableInputStreamConfiguration configuration =
        S3SeekableInputStreamConfiguration.builder()
            .physicalIOConfiguration(physicalIOConfiguration)
            .build();
    assertEquals(physicalIOConfiguration, configuration.getPhysicalIOConfiguration());
  }
}
