/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package software.amazon.s3.analyticsaccelerator.io.physical.data;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.concurrent.TimeUnit;
import lombok.NonNull;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.util.BlockKey;

/**
 * A cache implementation for storing and managing blob store index entries. This class provides a
 * wrapper around a Caffeine cache to store mappings between block keys and their corresponding
 * ranges in the blob store. The cache automatically expires entries based on access time and
 * maintains a maximum weight limit as specified in the configuration.
 */
public class BlobStoreIndexCache {
  /** The underlying Caffeine cache that stores block keys and their ranges */
  protected final Cache<BlockKey, Integer> indexCache;

  /**
   * Constructs a new BlobStoreIndexCache with the specified configuration. Initializes the cache
   * with expiration policies and size limits based on the configuration.
   *
   * @param configuration the PhysicalIO configuration containing cache settings
   */
  public BlobStoreIndexCache(@NonNull PhysicalIOConfiguration configuration) {
    this.indexCache =
        Caffeine.newBuilder()
            .expireAfterAccess(
                configuration.getBlobStoreTimeoutMilliseconds(), TimeUnit.MILLISECONDS)
            .weigher((BlockKey blockKey, Integer blockSize) -> blockSize)
            .maximumWeight(configuration.getBlobStoreCapacityBytes())
            .build();
  }

  /**
   * Stores a block key and its associated range value in the cache.
   *
   * @param blockKey the key identifying the block
   * @param range the range value associated with the block
   */
  public void put(BlockKey blockKey, int range) {
    indexCache.put(blockKey, range);
  }

  /**
   * Checks if a specific block key exists in the cache.
   *
   * @param blockKey the key to check for existence
   * @return true if the block key exists in the cache, false otherwise
   */
  public boolean contains(BlockKey blockKey) {
    return indexCache.asMap().containsKey(blockKey);
  }

  /**
   * Retrieves the range value associated with the specified block key from the cache, if it exists.
   *
   * @param blockKey the key whose associated range is to be returned
   */
  public void getIfPresent(BlockKey blockKey) {
    indexCache.getIfPresent(blockKey);
  }

  /**
   * Cleans up the cache by invalidating all entries and performing maintenance operations. This
   * method should be called when the cache needs to be cleared or during shutdown.
   */
  public void cleanUp() {
    if (indexCache != null) {
      indexCache.invalidateAll();
      indexCache.cleanUp();
    }
  }

  /**
   * Returns the maximum weight capacity configured for the index cache. This represents the upper
   * limit of memory that can be used by the cache before entries start being evicted.
   *
   * @return upper limit of memory that can be used by the cache before entries start being evicted.
   */
  public long getMaximumWeight() {
    return indexCache.policy().eviction().get().getMaximum();
  }

  /**
   * Returns the current total weight of all entries in the index cache. This represents the actual
   * memory usage of the cache at the time of calling. The value will always be less than or equal
   * to the maximum weight.
   *
   * @return the current weighted size of all entries in the cache
   */
  public long getCurrentWeight() {
    return indexCache.policy().eviction().get().weightedSize().getAsLong();
  }
}
