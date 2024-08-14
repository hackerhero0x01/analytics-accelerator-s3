package com.amazon.connector.s3.io.physical;

import static org.junit.jupiter.api.Assertions.*;

import com.amazon.connector.s3.S3SeekableInputStreamConfiguration;
import com.amazon.connector.s3.S3SeekableInputStreamConfigurationTest;
import com.amazon.connector.s3.common.ConnectorConfiguration;
import org.junit.jupiter.api.Test;

public class PhysicalIOConfigurationTest {

  @Test
  void testDefaultBuilder() {
    PhysicalIOConfiguration configuration = PhysicalIOConfiguration.builder().build();
    assertEquals(PhysicalIOConfiguration.DEFAULT, configuration);
  }

  @Test
  void testNonDefaults() {
    PhysicalIOConfiguration configuration =
        PhysicalIOConfiguration.builder().blobStoreCapacity(10).partSizeBytes(20).build();
    assertEquals(10, configuration.getBlobStoreCapacity());
    assertEquals(20, configuration.getPartSizeBytes());
  }

  @Test
  void testFromConfiguration() {
    ConnectorConfiguration configuration =
        S3SeekableInputStreamConfigurationTest.getConfiguration();
    ConnectorConfiguration mappedConfiguration =
        configuration.map(S3SeekableInputStreamConfiguration.PHYSICAL_IO_PREFIX);

    PhysicalIOConfiguration physicalIOConfiguration =
        PhysicalIOConfiguration.fromConfiguration(mappedConfiguration);

    assertEquals(10, physicalIOConfiguration.getMetadataStoreCapacity());
    assertEquals(20, physicalIOConfiguration.getBlockSizeBytes());
    // This should be equal to default since Property Prefix is not s3.connector.
    assertEquals(
        PhysicalIOConfiguration.DEFAULT.getBlobStoreCapacity(),
        physicalIOConfiguration.getBlobStoreCapacity());
  }
}
