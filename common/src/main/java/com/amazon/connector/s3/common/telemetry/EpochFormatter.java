package com.amazon.connector.s3.common.telemetry;

import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.NonNull;
import org.apache.logging.log4j.core.util.datetime.FastDateFormat;

/** Formatter used to output dates and times */
public final class EpochFormatter {
  private final @Getter @NonNull String pattern;
  private final @Getter @NonNull Locale locale;
  private final @Getter @NonNull TimeZone timeZone;
  private final @NonNull FastDateFormat dateFormat;

  /** Default pattern */
  public static final String DEFAULT_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
  /** Default locale. */
  public static final Locale DEFAULT_LOCALE = Locale.getDefault();
  /** Default time zone. */
  public static final TimeZone DEFAULT_TIMEZONE = TimeZone.getDefault();
  /** Default {@link EpochFormatter}. */
  public static EpochFormatter DEFAULT = new EpochFormatter();

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
    this.dateFormat = FastDateFormat.getInstance(pattern, timeZone, locale);
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
    return dateFormat.format(epochMillis);
  }

  /**
   * Formats the epoch timestamp measured in nanoseconds. This loses nano precision.
   *
   * @param epochNanos epoch.
   * @return formatted epoch
   */
  public String formatNanos(long epochNanos) {
    return dateFormat.format(TimeUnit.NANOSECONDS.toMillis(epochNanos));
  }
}
