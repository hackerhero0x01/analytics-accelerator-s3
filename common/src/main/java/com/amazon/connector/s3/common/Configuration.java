package com.amazon.connector.s3.common;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/** Configuration for Connector Framework for S3 */
public class Configuration {

  /** Expected prefix for properties related to Connector Framework for S3. * */
  private static final String PREFIX = "s3.connector.";

  private final Map<String, String> configuration;

  public static String getPrefix() {
    return PREFIX;
  }

  public Configuration(Map<String, String> upstreamConfiguration) {
    this.configuration =
        upstreamConfiguration.entrySet().stream()
            .filter(entry -> entry.getKey().startsWith(PREFIX))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public Configuration(Iterable<Map.Entry<String, String>> upstreamConfiguration) {
    this.configuration =
        StreamSupport.stream(upstreamConfiguration.spliterator(), false)
            .filter(entry -> entry.getKey().startsWith(PREFIX))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public int getInt(String key, int defaultValue) {
    String value = configuration.get(key);
    return value != null ? Integer.parseInt(value) : defaultValue;
  }

  public long getLong(String key, long defaultValue) {
    String value = configuration.get(key);
    return value != null ? Long.parseLong(value) : defaultValue;
  }

  public String getString(String key, String defaultValue) {
    String value = configuration.get(key);
    return value != null ? value : defaultValue;
  }

  public boolean getBoolean(String key, boolean defaultValue) {
    String value = configuration.get(key);
    return value != null ? Boolean.parseBoolean(value) : defaultValue;
  }

  public double getDouble(String key, double defaultValue) {
    String value = configuration.get(key);
    return value != null ? Double.parseDouble(value) : defaultValue;
  }
}
