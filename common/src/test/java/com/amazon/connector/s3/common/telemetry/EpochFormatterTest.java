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
package com.amazon.connector.s3.common.telemetry;

import static org.junit.jupiter.api.Assertions.*;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.ZoneId;
import java.util.Locale;
import java.util.TimeZone;
import org.junit.jupiter.api.Test;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class EpochFormatterTest {
  private static final long TEST_EPOCH_NANOS = 1722944779101123456L;

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
    String result = epochFormatter.formatNanos(TEST_EPOCH_NANOS);
    assertEquals("2024/08/06T17;46;19,101Z", result);
  }
}
