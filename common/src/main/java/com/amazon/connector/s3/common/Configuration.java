package com.amazon.connector.s3.common;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/** Configuration for Connector Framework for S3 */
public class Configuration {

  /** Expected prefix for properties related to Connector Framework for S3. * */
  private static final String PREFIX = "s3.connector.";

  private final Map<String, String> configuration;

  /**
   * Get prefix for properties related to Connector Framework for S3.
   *
   * @return String
   */
  public static String getPrefix() {
    return PREFIX;
  }

  /**
   * Constructs {@link Configuration}.
   *
   * @param upstreamConfiguration configuration from upstream service
   */
  public Configuration(Map<String, String> upstreamConfiguration) {
    this.configuration =
        upstreamConfiguration.entrySet().stream()
            .filter(entry -> entry.getKey().startsWith(PREFIX))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Constructs {@link Configuration}.
   *
   * @param upstreamConfiguration configuration from upstream service
   */
  public Configuration(Iterable<Map.Entry<String, String>> upstreamConfiguration) {
    this.configuration =
        StreamSupport.stream(upstreamConfiguration.spliterator(), false)
            .filter(entry -> entry.getKey().startsWith(PREFIX))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Get integer value for a given key. If key is not found, return default value.
   *
   * @param key
   * @param defaultValue
   * @return int
   */
  public int getInt(String key, int defaultValue) {
    String value = configuration.get(key);
    return value != null ? Integer.parseInt(value) : defaultValue;
  }

  /**
   * Get Long value for a given key. If key is not found, return default value.
   *
   * @param key
   * @param defaultValue
   * @return long
   */
  public long getLong(String key, long defaultValue) {
    String value = configuration.get(key);
    return value != null ? Long.parseLong(value) : defaultValue;
  }

  /**
   * Get String value for a given key. If key is not found, return default value.
   *
   * @param key
   * @param defaultValue
   * @return String
   */
  public String getString(String key, String defaultValue) {
    String value = configuration.get(key);
    return value != null ? value : defaultValue;
  }

  /**
   * Get Boolean value for a given key. If key is not found, return default value.
   *
   * @param key
   * @param defaultValue
   * @return boolean
   */
  public boolean getBoolean(String key, boolean defaultValue) {
    String value = configuration.get(key);
    return value != null ? Boolean.parseBoolean(value) : defaultValue;
  }

  /**
   * Get Double value for a given key. If key is not found, return default value.
   *
   * @param key
   * @param defaultValue
   * @return Double
   */
  public double getDouble(String key, double defaultValue) {
    String value = configuration.get(key);
    return value != null ? Double.parseDouble(value) : defaultValue;
  }
}
