package com.amazon.connector.s3.io.logical;

import static org.junit.jupiter.api.Assertions.*;

import com.amazon.connector.s3.common.Configuration;
import java.util.HashMap;
import java.util.Map;
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
        LogicalIOConfiguration.builder()
            .footerCachingEnabled(true)
            .footerCachingSize(10)
            .minPredictivePrefetchingConfidenceRatio(0.3)
            .build();
    assertTrue(configuration.isFooterCachingEnabled());
    assertEquals(10, configuration.getFooterCachingSize());
    assertEquals(0.3, configuration.getMinPredictivePrefetchingConfidenceRatio());
  }

  @Test
  void testFromConfiguration() {
    Map<String, String> properties = new HashMap<>();
    properties.put("s3.connector.logicalio.footer.caching.enabled", "false");
    properties.put("s3.connector.logicalio.footer.caching.size", "20");
    properties.put("invalidPrefix.logicalio.predictive.prefetching.enabled", "false");

    Configuration configuration = new Configuration(properties);
    LogicalIOConfiguration logicalIOConfiguration =
        LogicalIOConfiguration.fromConfiguration(configuration);

    assertFalse(logicalIOConfiguration.isFooterCachingEnabled());
    assertEquals(20, logicalIOConfiguration.getFooterCachingSize());
    // This should be equal to Default since Property Prefix is not s3.connector.
    assertEquals(
        LogicalIOConfiguration.DEFAULT.isPredictivePrefetchingEnabled(),
        logicalIOConfiguration.isPredictivePrefetchingEnabled());
  }
}
