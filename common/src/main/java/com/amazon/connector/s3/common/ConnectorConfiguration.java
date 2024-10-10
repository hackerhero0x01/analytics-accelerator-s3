  /*
   * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
   *
   * Licensed under the Apache License, Version 2.0 (the "License").
   * You may not use this file except in compliance with the License.
   * You may obtain a copy of the License at
   *
   *   http://www.apache.org/licenses/LICENSE-2.0
  
   * Unless required by applicable law or agreed to in writing, software
   * distributed under the License is distributed on an "AS IS" BASIS,
   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   * See the License for the specific language governing permissions and
   * limitations under the License.
   */
package com.amazon.connector.s3.common;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.Getter;
import lombok.NonNull;

/**
 * A map based Connector Framework Configuration to modify LogicalIO, PhysicalIO, and ObjectClient
 * settings. Each configuration item is a Key-Value pair, where keys start with a common prefix.
 * Constructors lets to pass this common prefix as well.
 *
 * <p>Example: Assume we have the following map fs.s3a.connector.logicalio.a = 10
 * fs.s3a.connector.logicalio.b = "foo" fs.s3a.connector.physicalio.a = 42
 * fs.s3a.connector.physicalio.y = "bar"
 *
 * <p>One can create a {@link ConnectorConfiguration} instance as follows: ConnectorConfiguration
 * conf = new ConnectorConfiguration(map, "fs.s3a.connector"); and getInt("physicalio.a", 0) will
 * return 42. Note that getter did not require initial prefix already passed to {@link
 * ConnectorConfiguration} (i.e. "fs.s3a.connector").
 *
 * <p>To avoid boilerplate prefix on the user side one can create a sub-configuration as follows:
 * MapBasedConfiguration subConf = conf.map("physicalio"); where parameter "physicalio" is the
 * append prefix, updating the search prefix to "fs.s3a.connector.physicalio". Then
 * subConf.getInt("a", 0) will return 42.
 */
public final class ConnectorConfiguration {
  /** Prefix identifying all values we are interested in. "" indicates all properties */
  @Getter private final String prefix;
  /** Underlying set of key/value pairs */
  private final Map<String, String> configuration;

  /**
   * Constructs {@link ConnectorConfiguration} from Map<String, String>. All keys are included in
   * the configuration
   *
   * @param configurationMap key/value set of {@link Map} representing the configuration
   */
  public ConnectorConfiguration(@NonNull Map<String, String> configurationMap) {
    this(configurationMap, "");
  }

  /**
   * Constructs {@link ConnectorConfiguration} from Map<String, String> and prependPrefix. Keys not
   * starting with prefix will be omitted from the configuration.
   *
   * @param configurationMap key/value set of {@link Map} representing the configuration
   * @param prefix prefix for properties related to Connector Framework for S3
   */
  public ConnectorConfiguration(
      @NonNull Map<String, String> configurationMap, @NonNull String prefix) {
    this(configurationMap.entrySet(), prefix);
  }

  /**
   * Constructs {@link ConnectorConfiguration} from Iterable of Map.Entry<String, String> and
   * prependPrefix. Keys not starting with prefix will be omitted from the configuration.
   *
   * @param iterableConfiguration key/value iterable of {@link Map} entries representing the
   *     configuration
   * @param prefix prefix for properties related to Connector Framework for S3
   */
  public ConnectorConfiguration(
      @NonNull Iterable<Map.Entry<String, String>> iterableConfiguration, @NonNull String prefix) {
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
  public ConnectorConfiguration map(@NonNull String appendPrefix) {
    return new ConnectorConfiguration(this.configuration, expandKey(appendPrefix));
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
  public int getInt(@NonNull String key, int defaultValue) throws NumberFormatException {
    return getValue(key, Integer::parseInt, () -> defaultValue);
  }

  /**
   * Get integer value for a given key. If key is not found, return default value. Note that this
   * method will throw an exception if the value is not a valid integer or the value is not set
   *
   * @param key suffix of the configuration to retrieve. Full search key will be this.getPrefix +
   *     "." + key
   * @return int
   */
  public int getRequiredInt(@NonNull String key) throws NumberFormatException {
    return getValue(key, Integer::parseInt, () -> throwIfNotPresent(key));
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
  public long getLong(@NonNull String key, long defaultValue) throws NumberFormatException {
    return getValue(key, Long::parseLong, () -> defaultValue);
  }

  /**
   * Get Long value for a given key. If key is not found, return default value. Note that this
   * method will throw an exception if the value is not a valid long or the value is not set
   *
   * @param key suffix of the configuration to retrieve. Full search key will be this.getPrefix +
   *     "." + key
   * @return long
   */
  public long getRequiredLong(@NonNull String key) throws NumberFormatException {
    return getValue(key, Long::parseLong, () -> throwIfNotPresent(key));
  }

  /**
   * Get String value for a given key. If key is not found, return default value.
   *
   * @param key suffix of the configuration to retrieve. Full search key will be this.getPrefix +
   *     "." + key
   * @param defaultValue default value if provided key does not exist
   * @return String
   */
  public String getString(@NonNull String key, String defaultValue) {
    return getValue(key, Function.identity(), () -> defaultValue);
  }

  /**
   * Get String value for a given key. If key is not found, return default value.
   *
   * @param key suffix of the configuration to retrieve. Full search key will be this.getPrefix +
   *     "." + key
   * @return String
   */
  public String getRequiredString(@NonNull String key) {
    return getValue(key, Function.identity(), () -> throwIfNotPresent(key));
  }

  /**
   * Get Boolean value for a given key. If key is not found, return default value.
   *
   * @param key suffix of the configuration to retrieve. Full search key will be this.getPrefix +
   *     "." + key
   * @param defaultValue default value if provided key does not exist
   * @return boolean
   */
  public boolean getBoolean(@NonNull String key, boolean defaultValue) {
    return getValue(key, Boolean::parseBoolean, () -> defaultValue);
  }

  /**
   * Get Boolean value for a given key. If key is not found, return default value.
   *
   * @param key suffix of the configuration to retrieve. Full search key will be this.getPrefix +
   *     "." + key
   * @return boolean
   */
  public boolean getRequiredBoolean(@NonNull String key) {
    return getValue(key, Boolean::parseBoolean, () -> throwIfNotPresent(key));
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
  public double getDouble(@NonNull String key, double defaultValue) throws NumberFormatException {
    return getValue(key, Double::parseDouble, () -> defaultValue);
  }

  /**
   * Get Double value for a given key. If key is not found, return default value. Note that this
   * method will throw an exception if the value is not a valid double or the value is not set.
   *
   * @param key suffix of the configuration to retrieve. Full search key will be this.getPrefix +
   *     "." + key
   * @return Double
   */
  public double getRequiredDouble(@NonNull String key) throws NumberFormatException {
    return getValue(key, Double::parseDouble, () -> throwIfNotPresent(key));
  }

  /**
   * Gets the value based on the key, then casts it to the desired type using the supplied 'caster'.
   * If the value is not set (that is, the map contains `null`), the value gets initialized using
   * the supplied `defaultValueSupplier`
   *
   * @param key suffix of the configuration to retrieve. Full search key will be this.getPrefix + *
   *     "." + key
   * @param caster function used to cast String to the desired type
   * @param defaultValueSupplier supplies default value when not set
   * @param <T> the desired type of the value
   * @return strongly typed result using the semantics above
   */
  private <T> T getValue(
      @NonNull String key, Function<String, T> caster, Supplier<T> defaultValueSupplier) {
    String value = configuration.get(expandKey(key));
    return (value != null) ? caster.apply(value) : defaultValueSupplier.get();
  }

  /**
   * Throws an exception when a given key is not present
   *
   * @param key the key
   * @param <T> the desired type of the value
   * @return nothing, exception is always thrown.
   */
  private static <T> T throwIfNotPresent(String key) {
    throw new IllegalArgumentException(
        String.format("Required configuration '%s' is not set.", key));
  }

  /**
   * Expands the supplied key by appending the prefix to it
   *
   * @param key the key
   * @return fully qualified key name
   */
  private String expandKey(String key) {
    // If the prefix is empty, do not attempt to expand
    if (this.prefix.isEmpty()) {
      return key;
    } else {
      return this.prefix + '.' + key;
    }
  }
}
