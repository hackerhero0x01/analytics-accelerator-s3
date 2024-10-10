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
package software.amazon.s3.dataaccelerator.common.telemetry;

import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.NonNull;

/** Formatter used to output dates and times */
public final class EpochFormatter {
  private final @Getter @NonNull String pattern;
  private final @Getter @NonNull Locale locale;
  private final @Getter @NonNull TimeZone timeZone;
  private final @NonNull ThreadLocal<SimpleDateFormat> dateFormat;

  /** Default pattern */
  public static final String DEFAULT_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
  /** Default locale. */
  public static final Locale DEFAULT_LOCALE = Locale.getDefault();
  /** Default time zone. */
  public static final TimeZone DEFAULT_TIMEZONE = TimeZone.getDefault();
  /** Default {@link EpochFormatter}. */
  public static final EpochFormatter DEFAULT = new EpochFormatter();

  /**
   * Creates a new instance of {@link EpochFormatter}.
   *
   * @param pattern format pattern for epochs.
   * @param timeZone time zone.
   * @param locale locale.
   */
  public EpochFormatter(
      @NonNull String pattern, @NonNull TimeZone timeZone, @NonNull Locale locale) {
    this.pattern = pattern;
    this.timeZone = timeZone;
    this.locale = locale;
    this.dateFormat =
        ThreadLocal.withInitial(
            () -> {
              SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern, locale);
              simpleDateFormat.setTimeZone(timeZone);
              return simpleDateFormat;
            });
  }

  /** Creates the {@link EpochFormatter} with sensible default. */
  public EpochFormatter() {
    this(DEFAULT_PATTERN, DEFAULT_TIMEZONE, DEFAULT_LOCALE);
  }

  /**
   * Formats the epoch timestamp measured in milliseconds.
   *
   * @param epochMillis epoch.
   * @return formatted epoch
   */
  public String formatMillis(long epochMillis) {
    return dateFormat.get().format(epochMillis);
  }

  /**
   * Formats the epoch timestamp measured in nanoseconds. This loses nano precision.
   *
   * @param epochNanos epoch.
   * @return formatted epoch
   */
  public String formatNanos(long epochNanos) {
    return dateFormat.get().format(TimeUnit.NANOSECONDS.toMillis(epochNanos));
  }
}
