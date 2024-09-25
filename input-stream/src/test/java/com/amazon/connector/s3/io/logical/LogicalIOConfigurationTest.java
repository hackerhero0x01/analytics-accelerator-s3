package com.amazon.connector.s3.io.logical;

import static org.junit.jupiter.api.Assertions.*;

import com.amazon.connector.s3.S3SeekableInputStreamConfiguration;
import com.amazon.connector.s3.S3SeekableInputStreamConfigurationTest;
import com.amazon.connector.s3.common.ConnectorConfiguration;
import org.junit.jupiter.api.Test;

public class LogicalIOConfigurationTest {

  @Test
  void testDefaultBuilder() {
    LogicalIOConfiguration configuration = LogicalIOConfiguration.builder().build();
    assertEquals(LogicalIOConfiguration.DEFAULT, configuration);
  }

  @Test
  void testNonDefaults() {
    LogicalIOConfiguration configuration =
        LogicalIOConfiguration.builder().footerCachingEnabled(true).footerCachingSize(10).build();
    assertTrue(configuration.isFooterCachingEnabled());
    assertEquals(10, configuration.getFooterCachingSize());
  }

  @Test
  void testFromConfiguration() {
    ConnectorConfiguration configuration =
        S3SeekableInputStreamConfigurationTest.getConfiguration();
    ConnectorConfiguration mappedConfiguration =
        configuration.map(S3SeekableInputStreamConfiguration.LOGICAL_IO_PREFIX);
    LogicalIOConfiguration logicalIOConfiguration =
        LogicalIOConfiguration.fromConfiguration(mappedConfiguration);

    assertFalse(logicalIOConfiguration.isFooterCachingEnabled());
    assertEquals(20, logicalIOConfiguration.getFooterCachingSize());
    // This should be equal to Default since Property Prefix is not s3.connector.
    assertEquals(
        LogicalIOConfiguration.DEFAULT.isPredictivePrefetchingEnabled(),
        logicalIOConfiguration.isPredictivePrefetchingEnabled());
  }
}
