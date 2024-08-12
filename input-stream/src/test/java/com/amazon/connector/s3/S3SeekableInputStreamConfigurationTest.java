package com.amazon.connector.s3;

import static com.amazon.connector.s3.S3SeekableInputStreamConfiguration.LOGICAL_IO_PREFIX;
import static com.amazon.connector.s3.S3SeekableInputStreamConfiguration.PHYSICAL_IO_PREFIX;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import com.amazon.connector.s3.common.ConnectorConfiguration;
import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.physical.PhysicalIOConfiguration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class S3SeekableInputStreamConfigurationTest {

  private static final String TEST_PREFIX = "s3.connector";

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

  @Test
  void testFromConfiguration() {
    ConnectorConfiguration configuration = getConfiguration();
    S3SeekableInputStreamConfiguration streamConfiguration =
        S3SeekableInputStreamConfiguration.fromConfiguration(configuration);

    assertNotNull(streamConfiguration.getLogicalIOConfiguration());
    assertFalse(streamConfiguration.getLogicalIOConfiguration().isFooterCachingEnabled());
    assertEquals(20, streamConfiguration.getLogicalIOConfiguration().getFooterCachingSize());
    // This should be equal to Default since Property Prefix is not s3.connector.
    assertEquals(
        LogicalIOConfiguration.DEFAULT.isPredictivePrefetchingEnabled(),
        streamConfiguration.getLogicalIOConfiguration().isPredictivePrefetchingEnabled());

    assertNotNull(streamConfiguration.getPhysicalIOConfiguration());
    assertEquals(10, streamConfiguration.getPhysicalIOConfiguration().getMetadataStoreCapacity());
    assertEquals(20, streamConfiguration.getPhysicalIOConfiguration().getBlockSizeBytes());
    // This should be equal to default since Property Prefix is not s3.connector.
    assertEquals(
        PhysicalIOConfiguration.DEFAULT.getBlobStoreCapacity(),
        streamConfiguration.getPhysicalIOConfiguration().getBlobStoreCapacity());
  }

  /**
   * Constructs {@link ConnectorConfiguration} object with test values.
   *
   * @return ConnectorConfiguration
   */
  public static ConnectorConfiguration getConfiguration() {
    Map<String, String> properties = new HashMap<>();
    properties.put(TEST_PREFIX + "." + LOGICAL_IO_PREFIX + ".footer.caching.enabled", "false");
    properties.put(TEST_PREFIX + "." + LOGICAL_IO_PREFIX + ".footer.caching.size", "20");
    properties.put("invalidPrefix.logicalio.predictive.prefetching.enabled", "false");
    properties.put(TEST_PREFIX + "." + PHYSICAL_IO_PREFIX + ".metadatastore.capacity", "10");
    properties.put(TEST_PREFIX + "." + PHYSICAL_IO_PREFIX + ".blocksizebytes", "20");
    properties.put("invalidPrefix.physicalio.blobstore.capacity", "3");

    return new ConnectorConfiguration(properties, TEST_PREFIX);
  }
}
