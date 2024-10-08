package com.amazon.connector.s3.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class PrefetchModeTest {

  @Test
  public void testPrefetchModeFromString() {
    assertEquals(PrefetchMode.ALL, PrefetchMode.fromString("aLL"));
    assertEquals(PrefetchMode.OFF, PrefetchMode.fromString("OFF"));
    assertEquals(PrefetchMode.COLUMN_BOUND, PrefetchMode.fromString("column_bound"));
    assertEquals(PrefetchMode.ROW_GROUP, PrefetchMode.fromString("row_group"));

    // defaults to ROW_GROUP mode
    assertEquals(PrefetchMode.ROW_GROUP, PrefetchMode.fromString("xyz"));
  }
}
