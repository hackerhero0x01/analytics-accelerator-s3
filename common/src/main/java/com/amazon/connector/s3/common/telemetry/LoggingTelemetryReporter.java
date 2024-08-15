package com.amazon.connector.s3.common.telemetry;

import lombok.Getter;
import lombok.NonNull;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This {@link TelemetryReporter} outputs telemetry into a log with a given name and level. {@link
 * LoggingTelemetryReporter#DEFAULT_LOGGING_NAME} and {@link
 * LoggingTelemetryReporter#DEFAULT_LOGGING_LEVEL} are used by default.
 */
class LoggingTelemetryReporter implements TelemetryReporter {
  @Getter @NonNull private final EpochFormatter epochFormatter;
  @Getter @NonNull private final String loggerName;
  @Getter @NonNull private final Level loggerLevel;
  @NonNull private final Logger logger;

  /** Default logging loggerLevel */
  public static Level DEFAULT_LOGGING_LEVEL = Level.INFO;

  /** Default logger name */
  public static String DEFAULT_LOGGING_NAME = "com.amazon.connector.s3.telemetry";

  /** Creates a new instance of {@link LoggingTelemetryReporter} with sensible defaults. */
  public LoggingTelemetryReporter() {
    this(DEFAULT_LOGGING_NAME, DEFAULT_LOGGING_LEVEL, EpochFormatter.DEFAULT);
  }

  /**
   * Creates a new instance of {@link LoggingTelemetryReporter}.
   *
   * @param loggerName logger name.
   * @param loggerLevel logger level.
   * @param epochFormatter an instance of {@link EpochFormatter to use to format epochs}.
   */
  public LoggingTelemetryReporter(
      @NonNull String loggerName,
      @NonNull Level loggerLevel,
      @NonNull EpochFormatter epochFormatter) {
    this.loggerName = loggerName;
    this.epochFormatter = epochFormatter;
    this.loggerLevel = loggerLevel;
    this.logger = LogManager.getLogger(loggerName);
  }

  /**
   * Reports the start of an operation
   *
   * @param epochTimestampNanos wall clock time for the operation start
   * @param operation and instance of {@link Operation} to start
   */
  @Override
  public void reportStart(long epochTimestampNanos, Operation operation) {
    this.logger.log(
        this.loggerLevel,
        OperationMeasurement.getOperationStartingString(
            operation, epochTimestampNanos, this.epochFormatter));
  }

  /**
   * Outputs the current contents of {@link OperationMeasurement} into a log.
   *
   * @param operationMeasurement operation execution.
   */
  @Override
  public void reportComplete(@NonNull OperationMeasurement operationMeasurement) {
    String message = operationMeasurement.toString(epochFormatter);
    if (operationMeasurement.getError().isPresent()) {
      // If the operation failed, always record as error.
      this.logger.log(Level.ERROR, message, operationMeasurement.getError().get());
    } else {
      this.logger.log(this.loggerLevel, message);
    }
  }
}
