package com.amazon.connector.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazon.connector.s3.common.ConnectorConfiguration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class ObjectClientConfigurationTest {
  private static final String CONFIGURATION_PREFIX = "s3.connector";
  private static final String PROPERTY_PREFIX = "objectclient";

  @Test
  void testDefaultBuilder() {
    ObjectClientConfiguration configuration = ObjectClientConfiguration.builder().build();
    assertEquals(ObjectClientConfiguration.DEFAULT, configuration);
  }

  @Test
  void testNonDefaults() {
    ObjectClientConfiguration configuration =
        ObjectClientConfiguration.builder().userAgentPrefix("newUserAgent").build();
    assertEquals("newUserAgent", configuration.getUserAgentPrefix());
  }

  @Test
  void testFromConfiguration() {
    Map<String, String> properties = new HashMap<>();
    properties.put("s3.connector.objectclient.useragentprefix", "newUserAgent");

    ConnectorConfiguration configuration =
        new ConnectorConfiguration(properties, CONFIGURATION_PREFIX);
    configuration = configuration.map(PROPERTY_PREFIX);
    ObjectClientConfiguration objectClientConfiguration =
        ObjectClientConfiguration.fromConfiguration(configuration);
    assertEquals("newUserAgent", objectClientConfiguration.getUserAgentPrefix());
  }
}
