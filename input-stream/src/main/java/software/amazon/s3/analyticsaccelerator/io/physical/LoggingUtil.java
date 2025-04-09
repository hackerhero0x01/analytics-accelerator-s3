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
package software.amazon.s3.analyticsaccelerator.io.physical;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;

/** Utility class for structured logging with timing and thread information. */
public class LoggingUtil {
  private static final String TIME_MS = "TIME_MS";
  private static final String DURATION_MS = "DURATION_MS";
  private static final String THREAD_ID = "THREAD_ID";
  private static final String THREAD_NAME = "THREAD_NAME";

  /** Builder class for constructing structured log messages. */
  public static class LogBuilder {
    private final Logger logger;
    private final String operation;
    private final Map<String, Object> params;
    private long startTimeMs;
    private boolean includeThread;
    private boolean includeTiming;

    /**
     * Constructs a new LogBuilder.
     *
     * @param logger The SLF4J logger to use
     * @param operation The operation name to include in log messages
     */
    private LogBuilder(Logger logger, String operation) {
      this.logger = logger;
      this.operation = operation;
      this.params = new LinkedHashMap<>();
      this.startTimeMs = System.currentTimeMillis();
      this.includeThread = false;
      this.includeTiming = false;
    }

    /**
     * Adds a parameter to the log message.
     *
     * @param key The parameter key
     * @param value The parameter value
     * @return this builder for method chaining
     */
    public LogBuilder withParam(String key, Object value) {
      params.put(key, value);
      return this;
    }

    /**
     * Includes thread information in the log message.
     *
     * @return this builder for method chaining
     */
    public LogBuilder withThreadInfo() {
      this.includeThread = true;
      return this;
    }

    /**
     * Includes timing information in the log message.
     *
     * @return this builder for method chaining
     */
    public LogBuilder withTiming() {
      this.includeTiming = true;
      return this;
    }

    /** Adds thread information to the parameters map. */
    private void addThreadInfo() {
      params.put(THREAD_ID, Thread.currentThread().getId());
      params.put(THREAD_NAME, Thread.currentThread().getName());
    }

    /**
     * Adds timing information to the parameters map.
     *
     * @param isEnd whether this is an end log (to include duration)
     */
    private void addTiming(boolean isEnd) {
      params.put(TIME_MS, System.currentTimeMillis());
      if (isEnd) {
        params.put(DURATION_MS, System.currentTimeMillis() - startTimeMs);
      }
    }

    /** Logs the start of an operation. */
    public void logStart() {
      if (includeThread) addThreadInfo();
      if (includeTiming) addTiming(false);

      String message = String.format("STARTED %s: %s", operation, buildParamString());
      logger.debug(message);
    }

    /** Logs the end of an operation. */
    public void logEnd() {
      if (includeThread) addThreadInfo();
      if (includeTiming) addTiming(true);

      String message = String.format("DONE %s: %s", operation, buildParamString());
      logger.debug(message);
    }

    /**
     * Builds the parameter string for the log message.
     *
     * @return formatted string of all parameters
     */
    private String buildParamString() {
      List<String> paramStrings = new ArrayList<>();
      for (Map.Entry<String, Object> entry : params.entrySet()) {
        String paramString = String.format("%s: %s", entry.getKey(), entry.getValue());
        paramStrings.add(paramString);
      }
      return String.join(", ", paramStrings);
    }
  }

  /**
   * Creates a new LogBuilder instance.
   *
   * @param logger The SLF4J logger to use
   * @param operation The operation name to include in log messages
   * @return A new LogBuilder instance
   */
  public static LogBuilder start(Logger logger, String operation) {
    return new LogBuilder(logger, operation);
  }
}
