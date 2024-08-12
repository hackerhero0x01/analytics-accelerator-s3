package com.amazon.connector.s3.common.telemetry;

import static org.junit.jupiter.api.Assertions.*;

import java.time.ZoneId;
import java.util.Locale;
import java.util.TimeZone;
import org.junit.jupiter.api.Test;

public class EpochFormatterTest {
  @Test
  public void testCreateWithArguments() {
    EpochFormatter epochFormatter =
        new EpochFormatter(
            "yyyy/MM/dd'T'HH;mm;ss,SSS'Z'",
            TimeZone.getTimeZone(ZoneId.of("BST", ZoneId.SHORT_IDS)),
            Locale.ENGLISH);
    assertEquals(
        TimeZone.getTimeZone(ZoneId.of("BST", ZoneId.SHORT_IDS)), epochFormatter.getTimeZone());
    assertEquals(Locale.ENGLISH, epochFormatter.getLocale());
    assertEquals("yyyy/MM/dd'T'HH;mm;ss,SSS'Z'", epochFormatter.getPattern());
  }

  @Test
  public void testCreateDefault() {
    EpochFormatter epochFormatter = new EpochFormatter();
    assertEquals(EpochFormatter.DEFAULT_TIMEZONE, epochFormatter.getTimeZone());
    assertEquals(EpochFormatter.DEFAULT_LOCALE, epochFormatter.getLocale());
    assertEquals(EpochFormatter.DEFAULT_PATTERN, epochFormatter.getPattern());

    assertEquals(EpochFormatter.DEFAULT.getTimeZone(), epochFormatter.getTimeZone());
    assertEquals(EpochFormatter.DEFAULT.getLocale(), epochFormatter.getLocale());
    assertEquals(EpochFormatter.DEFAULT.getPattern(), epochFormatter.getPattern());
  }

  @Test
  public void testCreateThrowsOnNulls() {
    assertThrows(
        NullPointerException.class,
        () -> {
          EpochFormatter epochFormatter =
              new EpochFormatter(
                  null, TimeZone.getTimeZone(ZoneId.of("BST", ZoneId.SHORT_IDS)), Locale.ENGLISH);
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          EpochFormatter epochFormatter =
              new EpochFormatter("yyyy/MM/dd'T'HH;mm;ss,SSS'Z'", null, Locale.ENGLISH);
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          EpochFormatter epochFormatter =
              new EpochFormatter(
                  "yyyy/MM/dd'T'HH;mm;ss,SSS'Z'",
                  TimeZone.getTimeZone(ZoneId.of("BST", ZoneId.SHORT_IDS)),
                  null);
        });
  }

  @Test
  void testFormatMillis() {
    EpochFormatter epochFormatter =
        new EpochFormatter(
            "yyyy/MM/dd'T'HH;mm;ss,SSS'Z'",
            TimeZone.getTimeZone(ZoneId.of("BST", ZoneId.SHORT_IDS)),
            Locale.ENGLISH);
    String result = epochFormatter.formatMillis(1722944779101L);
    assertEquals("2024/08/06T17;46;19,101Z", result);
  }

  @Test
  void testFormatNanos() {
    EpochFormatter epochFormatter =
        new EpochFormatter(
            "yyyy/MM/dd'T'HH;mm;ss,SSS'Z'",
            TimeZone.getTimeZone(ZoneId.of("BST", ZoneId.SHORT_IDS)),
            Locale.ENGLISH);
    String result = epochFormatter.formatNanos(1722944779101123456L);
    assertEquals("2024/08/06T17;46;19,101Z", result);
  }
}
