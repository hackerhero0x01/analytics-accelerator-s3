package com.amazon.connector.s3.common;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.Getter;

/**
 * A map based Connector Framework Configuration to modify LogicalIO, PhysicalIO, and ObjectClient
 * for settings. Each configuration item is a Key-Value pair, where keys start with a common prefix.
 * Constructors lets to pass this common prefix as well.
 *
 * <p>Example: Assume we have the following map fs.s3a.connector.logicalio.a = 10
 * fs.s3a.connector.logicalio.b = "foo" fs.s3a.connector.physicalio.a = 42
 * fs.s3a.connector.physicalio.y = "bar"
 *
 * <p>One can create a {@link ConnectorConfiguration} instance as follows: MapBasedConfiguration
 * conf = new MapBasedConfiguration(map, "fs.s3a.connector"); and getInt("physicalio.a", 0) will
 * return 42. Note that getter did not require initial prefix already passed to {@link
 * ConnectorConfiguration} (i.e. "fs.s3a.connector").
 *
 * <p>To avoid boilerplate prefix on the user side one can create a sub-configuration as follows:
 * MapBasedConfiguration subConf = conf.map("physicalio"); where parameter "physicalio" is the
 * append prefix, updating the search prefix to "fs.s3a.connector.physicalio". Then
 * subConf.getInt("a", 0) will return 42.
 */
public class ConnectorConfiguration {

  /**
   * Expected prefix for properties related to Connector Framework for S3. * Get prefix for
   * properties related to Connector Framework for S3.
   *
   * @return String
   */
  @Getter private final String prefix;

  private final Map<String, String> configuration;

  /**
   * Constructs {@link ConnectorConfiguration} from Map<String, String> and prependPrefix. Keys not
   * starting with prefix will be omitted from the map.
   *
   * @param configurationMap configuration from upstream service
   * @param prefix prefix for properties related to Connector Framework for S3
   */
  public ConnectorConfiguration(Map<String, String> configurationMap, String prefix) {
    this(configurationMap.entrySet(), prefix);
  }

  /**
   * Constructs {@link ConnectorConfiguration} from Iterable of Map.Entry<String, String> and
   * prependPrefix. Keys not starting with prefix will be omitted from the map.
   *
   * @param iterableConfiguration Iterable of Map.Entry<String, String> from user
   * @param prefix prefix for properties related to Connector Framework for S3
   */
  public ConnectorConfiguration(
      Iterable<Map.Entry<String, String>> iterableConfiguration, String prefix) {
    this.prefix = prefix;
    this.configuration =
        StreamSupport.stream(iterableConfiguration.spliterator(), false)
            .filter(entry -> entry.getKey().startsWith(getPrefix()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Return a new {@link ConnectorConfiguration} where for common prefix for keys is updated to
   * this.getPrefix() + "." + appendPrefix
   *
   * @param appendPrefix prefix to append to the common prefix for keys
   * @return {@link ConnectorConfiguration}
   */
  public ConnectorConfiguration map(String appendPrefix) {
    return new ConnectorConfiguration(this.configuration, constructKey(appendPrefix));
  }

  /**
   * Get integer value for a given key. If key is not found, return default value. Note that this
   * method will throw an exception if the value is not a valid integer.
   *
   * @param key suffix of the configuration to retrieve. Full search key will be this.getPrefix +
   *     "." + key
   * @param defaultValue default value if provided key does not exist
   * @return int
   */
  public int getInt(String key, int defaultValue) throws NumberFormatException {
    String value = configuration.get(constructKey(key));
    return value != null ? Integer.parseInt(value) : defaultValue;
  }

  /**
   * Get Long value for a given key. If key is not found, return default value. Note that this
   * method will throw an exception if the value is not a valid long.
   *
   * @param key suffix of the configuration to retrieve. Full search key will be this.getPrefix +
   *     "." + key
   * @param defaultValue default value if provided key does not exist
   * @return long
   */
  public long getLong(String key, long defaultValue) throws NumberFormatException {
    String value = configuration.get(constructKey(key));
    return value != null ? Long.parseLong(value) : defaultValue;
  }

  /**
   * Get String value for a given key. If key is not found, return default value.
   *
   * @param key suffix of the configuration to retrieve. Full search key will be this.getPrefix +
   *     "." + key
   * @param defaultValue default value if provided key does not exist
   * @return String
   */
  public String getString(String key, String defaultValue) {
    String value = configuration.get(constructKey(key));
    return value != null ? value : defaultValue;
  }

  /**
   * Get Boolean value for a given key. If key is not found, return default value.
   *
   * @param key suffix of the configuration to retrieve. Full search key will be this.getPrefix +
   *     "." + key
   * @param defaultValue default value if provided key does not exist
   * @return boolean
   */
  public boolean getBoolean(String key, boolean defaultValue) {
    String value = configuration.get(constructKey(key));
    return value != null ? Boolean.parseBoolean(value) : defaultValue;
  }

  /**
   * Get Double value for a given key. If key is not found, return default value. Note that this
   * method will throw an exception if the value is not a valid double.
   *
   * @param key suffix of the configuration to retrieve. Full search key will be this.getPrefix +
   *     "." + key
   * @param defaultValue default value if provided key does not exist
   * @return Double
   */
  public double getDouble(String key, double defaultValue) throws NumberFormatException {
    String value = configuration.get(constructKey(key));
    return value != null ? Double.parseDouble(value) : defaultValue;
  }

  private String constructKey(String key) {
    return this.prefix + '.' + key;
  }
}
