package com.amazon.connector.s3.common;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class ConnectorConfigurationTest {

  private static final String TEST_PREFIX = "s3.connector";
  private static final String MAP_PREFIX = "map";

  @Test
  void testConstructorWithMap() {
    ConnectorConfiguration configuration =
        new ConnectorConfiguration(getDefaultConfigurationMap(TEST_PREFIX), TEST_PREFIX);

    assertNotNull(configuration);
    assertEquals("stringConfigValue", configuration.getString("stringConfig", "randomString"));
  }

  @Test
  void testConstructorWithIterable() {
    ConnectorConfiguration configuration =
        new ConnectorConfiguration(getDefaultConfigurationIterator(TEST_PREFIX), TEST_PREFIX);

    assertNotNull(configuration);
    assertEquals("stringConfigValue", configuration.getString("stringConfig", "randomString"));
  }

  @Test
  void testMapPrefix() {
    ConnectorConfiguration configuration =
        new ConnectorConfiguration(
            getDefaultConfigurationMap(TEST_PREFIX + "." + MAP_PREFIX), TEST_PREFIX);
    ConnectorConfiguration mappedConfiguration = configuration.map(MAP_PREFIX);

    assertNotNull(mappedConfiguration);
    assertEquals(
        "stringConfigValue", mappedConfiguration.getString("stringConfig", "randomString"));
  }

  @Test
  void testFilterWorksWithMap() {
    Map<String, String> defaultMap = getDefaultConfigurationMap(TEST_PREFIX);
    defaultMap.put("key", "value");
    ConnectorConfiguration configuration = new ConnectorConfiguration(defaultMap, TEST_PREFIX);

    assertNotNull(configuration);
    assertEquals("randomString", configuration.getString("key", "randomString"));
  }

  @Test
  void testReturnsDefault() {
    ConnectorConfiguration configuration = getDefaultConfiguration(TEST_PREFIX);
    assertEquals(configuration.getInt("KeyNotExists", 1), 1);
    assertEquals(configuration.getDouble("KeyNotExists", 1.0), 1.0);
    assertTrue(configuration.getBoolean("KeyNotExists", true));
    assertFalse(configuration.getBoolean("KeyNotExists", false));
    assertEquals(0L, configuration.getLong("KeyNotExists", 0));
    assertEquals(configuration.getString("KeyNotExists", "randomstring"), "randomstring");
  }

  @Test
  void testGetInt() {
    ConnectorConfiguration configuration = getDefaultConfiguration(TEST_PREFIX);
    int intConfig = configuration.getInt("intConfig", 0);
    assertEquals(1, intConfig);
  }

  @Test
  void testGetIntThrows() {
    ConnectorConfiguration configuration = getDefaultConfiguration(TEST_PREFIX);
    assertThrows(NumberFormatException.class, () -> configuration.getInt("stringConfig", 0));
  }

  @Test
  void testGetLong() {
    ConnectorConfiguration configuration = getDefaultConfiguration(TEST_PREFIX);
    long longConfig = configuration.getLong("longConfig", 0);
    assertEquals(1, longConfig);
  }

  @Test
  void testGetLongThrows() {
    ConnectorConfiguration configuration = getDefaultConfiguration(TEST_PREFIX);
    assertThrows(NumberFormatException.class, () -> configuration.getLong("stringConfig", 0));
  }

  @Test
  void testGetDouble() {
    ConnectorConfiguration configuration = getDefaultConfiguration(TEST_PREFIX);
    double doubleConfig = configuration.getDouble("doubleConfig", 0.0);
    assertEquals(1.0, doubleConfig);
  }

  @Test
  void testGetDoubleThrows() {
    ConnectorConfiguration configuration = getDefaultConfiguration(TEST_PREFIX);
    assertThrows(NumberFormatException.class, () -> configuration.getDouble("stringConfig", 0.0));
  }

  @Test
  void testGetBoolean() {
    ConnectorConfiguration configuration = getDefaultConfiguration(TEST_PREFIX);
    boolean booleanConfig = configuration.getBoolean("booleanConfig", false);
    assertTrue(booleanConfig);
  }

  @Test
  void testGetBooleanReturnsFalse() {
    ConnectorConfiguration configuration = getDefaultConfiguration(TEST_PREFIX);
    assertFalse(configuration.getBoolean("stringConfig", true));
  }

  @Test
  void testGetString() {
    ConnectorConfiguration configuration = getDefaultConfiguration(TEST_PREFIX);
    String stringConfig = configuration.getString("stringConfig", "randomString");
    assertInstanceOf(String.class, stringConfig);
    assertEquals("stringConfigValue", stringConfig);
  }

  private static ConnectorConfiguration getDefaultConfiguration(String prefix) {

    return new ConnectorConfiguration(getDefaultConfigurationMap(prefix), prefix);
  }

  private static Map<String, String> getDefaultConfigurationMap(String prefix) {
    Map<String, String> defaultMap = new HashMap<>();
    defaultMap.put(prefix + ".intConfig", "1");
    defaultMap.put(prefix + ".doubleConfig", "1.0");
    defaultMap.put(prefix + ".longConfig", "1");
    defaultMap.put(prefix + ".booleanConfig", "true");
    defaultMap.put(prefix + ".stringConfig", "stringConfigValue");
    return defaultMap;
  }

  private static Set<Map.Entry<String, String>> getDefaultConfigurationIterator(String prefix) {
    return getDefaultConfigurationMap(prefix).entrySet();
  }
}
