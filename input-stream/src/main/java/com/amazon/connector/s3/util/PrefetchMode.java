package com.amazon.connector.s3.util;

public enum PrefetchMode {
  OFF("off"),
  ALL("all"),
  CAUTIOUS("cautious"),
  COLUMN_BOUND("column_bound");

  private final String name;

  PrefetchMode(String name) {
    this.name = name;
  }

  public boolean equalsValue(String prefetchMode) {
    return this.name.equalsIgnoreCase(prefetchMode);
  }
}
