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
package software.amazon.s3.analyticsaccelerator.common.telemetry;

import java.util.Optional;
import lombok.Getter;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import software.amazon.s3.analyticsaccelerator.util.LogHelper;

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
  @NonNull private final TelemetryFormat telemetryFormat;

  /** Default logging loggerLevel */
  public static Level DEFAULT_LOGGING_LEVEL = Level.INFO;

  /** Default logger name */
  public static String DEFAULT_LOGGING_NAME = "com.amazon.connector.s3.telemetry";

  /** Creates a new instance of {@link LoggingTelemetryReporter} with sensible defaults. */
  public LoggingTelemetryReporter() {
    this(
        DEFAULT_LOGGING_NAME,
        DEFAULT_LOGGING_LEVEL,
        EpochFormatter.DEFAULT,
        new DefaultTelemetryFormat());
  }

  /**
   * Creates a new instance of {@link LoggingTelemetryReporter}.
   *
   * @param loggerName logger name.
   * @param loggerLevel logger level.
   * @param epochFormatter an instance of {@link EpochFormatter to use to format epochs}.
   * @param telemetryFormat an instance of {@link TelemetryFormat to use to format telemetry with}
   */
  public LoggingTelemetryReporter(
      @NonNull String loggerName,
      @NonNull Level loggerLevel,
      @NonNull EpochFormatter epochFormatter,
      @NonNull TelemetryFormat telemetryFormat) {
    this.loggerName = loggerName;
    this.epochFormatter = epochFormatter;
    this.loggerLevel = loggerLevel;
    this.logger = LoggerFactory.getLogger(loggerName);
    this.telemetryFormat = telemetryFormat;
  }

  /**
   * Reports the start of an operation
   *
   * @param epochTimestampNanos wall clock time for the operation start
   * @param operation and instance of {@link Operation} to start
   */
  @Override
  public void reportStart(long epochTimestampNanos, Operation operation) {
    LogHelper.logAtLevel(
        this.logger,
        this.loggerLevel,
        this.telemetryFormat.renderOperationStart(operation, epochTimestampNanos, epochFormatter),
        Optional.empty());
  }

  /**
   * Outputs the current contents of {@link OperationMeasurement} into a log.
   *
   * @param datapointMeasurement operation execution.
   */
  @Override
  public void reportComplete(@NonNull TelemetryDatapointMeasurement datapointMeasurement) {
    String message = datapointMeasurement.toString(telemetryFormat, epochFormatter);
    if (datapointMeasurement instanceof OperationMeasurement) {
      OperationMeasurement operationMeasurement = (OperationMeasurement) datapointMeasurement;
      if (operationMeasurement.getError().isPresent()) {
        // If the operation failed, always record as error.
        LogHelper.logAtLevel(this.logger, Level.ERROR, message, operationMeasurement.getError());
        return;
      }
    }
    LogHelper.logAtLevel(this.logger, this.loggerLevel, message, Optional.empty());
  }

  /** Flushes any intermediate state of the reporters In this case, this is a no-op */
  @Override
  public void flush() {}
}
