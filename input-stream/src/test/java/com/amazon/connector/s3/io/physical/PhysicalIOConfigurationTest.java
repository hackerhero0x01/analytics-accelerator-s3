package com.amazon.connector.s3.io.physical;

import static org.junit.jupiter.api.Assertions.*;

import com.amazon.connector.s3.common.Configuration;
import java.util.HashMap;
import java.util.Map;
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
    Map<String, String> properties = new HashMap<>();
    properties.put("s3.connector.physicalio.metadatastore.capacity", "10");
    properties.put("s3.connector.physicalio.blocksizebytes", "20");
    properties.put("invalidPrefix.physicalio.blobstore.capacity", "3");

    Configuration configuration = new Configuration(properties);
    PhysicalIOConfiguration physicalIOConfiguration =
        PhysicalIOConfiguration.fromConfiguration(configuration);

    assertEquals(10, physicalIOConfiguration.getMetadataStoreCapacity());
    assertEquals(20, physicalIOConfiguration.getBlockSizeBytes());
    // This should be equal to default since Property Prefix is not s3.connector.
    assertEquals(
        PhysicalIOConfiguration.DEFAULT.getBlobStoreCapacity(),
        physicalIOConfiguration.getBlobStoreCapacity());
  }
}
