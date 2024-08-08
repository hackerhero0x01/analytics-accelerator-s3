package com.amazon.connector.s3.common;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class ConfigurationTest {

  @Test
  void testConstructorWithMap() {
    Configuration configuration = new Configuration(getDefaultConfigurationMap());

    assertNotNull(configuration);
    assertEquals(
        "stringConfigValue",
        configuration.getString(Configuration.getPrefix() + "." + "stringConfig", "randomString"));
  }

  @Test
  void testConstructorWithIterable() {
    Configuration configuration = new Configuration(this::getDefaultConfigurationIterator);

    assertNotNull(configuration);
    assertEquals(
        "stringConfigValue",
        configuration.getString(Configuration.getPrefix() + "." + "stringConfig", "randomString"));
  }

  @Test
  void testFilterWorksWithMap() {
    Map<String, String> defaultMap = getDefaultConfigurationMap();
    defaultMap.put("key", "value");
    Configuration configuration = new Configuration(defaultMap);

    assertNotNull(configuration);
    assertEquals(
        "stringConfigValue",
        configuration.getString(Configuration.getPrefix() + "." + "stringConfig", "randomString"));
  }

  @Test
  void testReturnsDefault() {
    Configuration configuration = new Configuration(getDefaultConfigurationMap());
    assertEquals(configuration.getInt("KeyNotExists", 1), 1);
    assertEquals(configuration.getDouble("KeyNotExists", 1.0), 1.0);
    assertTrue(configuration.getBoolean("KeyNotExists", true));
    assertFalse(configuration.getBoolean("KeyNotExists", false));
    assertEquals(0L, configuration.getLong("KeyNotExists", 0));
    assertEquals(configuration.getString("KeyNotExists", "randomstring"), "randomstring");
  }

  @Test
  void testGetInt() {
    Configuration configuration = new Configuration(getDefaultConfigurationMap());
    int intConfig = configuration.getInt(Configuration.getPrefix() + "." + "intConfig", 0);
    assertInstanceOf(Integer.class, intConfig);
    assertEquals(1, intConfig);
  }

  @Test
  void testGetLong() {
    Configuration configuration = new Configuration(getDefaultConfigurationMap());
    long longConfig = configuration.getLong(Configuration.getPrefix() + "." + "longConfig", 0);
    assertInstanceOf(Long.class, longConfig);
    assertEquals(1, longConfig);
  }

  @Test
  void testGetDouble() {
    Configuration configuration = new Configuration(getDefaultConfigurationMap());
    double doubleConfig =
        configuration.getDouble(Configuration.getPrefix() + "." + "doubleConfig", 0.0);
    assertInstanceOf(Double.class, doubleConfig);
    assertEquals(1.0, doubleConfig);
  }

  @Test
  void testGetBoolean() {
    Configuration configuration = new Configuration(getDefaultConfigurationMap());
    boolean booleanConfig =
        configuration.getBoolean(Configuration.getPrefix() + "." + "booleanConfig", false);
    assertInstanceOf(Boolean.class, booleanConfig);
    assertTrue(booleanConfig);
  }

  @Test
  void testGetString() {
    Configuration configuration = new Configuration(getDefaultConfigurationMap());
    String stringConfig =
        configuration.getString(Configuration.getPrefix() + "." + "stringConfig", "randomString");
    assertInstanceOf(String.class, stringConfig);
    assertEquals("stringConfigValue", stringConfig);
  }

  private Map<String, String> getDefaultConfigurationMap() {
    Map<String, String> defaultMap = new HashMap<>();
    defaultMap.put(Configuration.getPrefix() + "." + "intConfig", "1");
    defaultMap.put(Configuration.getPrefix() + "." + "doubleConfig", "1.0");
    defaultMap.put(Configuration.getPrefix() + "." + "longConfig", "1");
    defaultMap.put(Configuration.getPrefix() + "." + "booleanConfig", "true");
    defaultMap.put(Configuration.getPrefix() + "." + "stringConfig", "stringConfigValue");
    return defaultMap;
  }

  private Iterator<Map.Entry<String, String>> getDefaultConfigurationIterator() {
    return getDefaultConfigurationMap().entrySet().iterator();
  }
}
