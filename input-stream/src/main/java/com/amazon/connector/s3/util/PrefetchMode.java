package com.amazon.connector.s3.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class defining prefetch modes. */
public enum PrefetchMode {
  OFF("off"),
  ALL("all"),
  ROW_GROUP("row_group"),
  COLUMN_BOUND("column_bound");

  private final String name;

  private static final Logger LOG = LoggerFactory.getLogger(PrefetchMode.class);

  PrefetchMode(String name) {
    this.name = name;
  }

  public static PrefetchMode fromString(String prefetchMode) {
    for (PrefetchMode value : values()) {
      if (value.name.equalsIgnoreCase(prefetchMode)) {
        return value;
      }
    }
    LOG.warn("Unknown prefetch mode {}, using default row_group mode.", prefetchMode);

    return ROW_GROUP;
  }
}
