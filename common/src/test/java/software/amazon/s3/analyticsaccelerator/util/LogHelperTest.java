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
package software.amazon.s3.analyticsaccelerator.util;

import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.event.Level;

class LogHelperTest {

  @Mock private Logger logger;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  void testLogErrorWithException() {
    String message = "Error message";
    Exception exception = new RuntimeException("Test exception");

    LogHelper.logAtLevel(logger, Level.ERROR, message, Optional.of(exception));

    verify(logger).error(message, exception);
    verifyNoMoreInteractions(logger);
  }

  @Test
  void testLogErrorWithoutException() {
    String message = "Error message";

    LogHelper.logAtLevel(logger, Level.ERROR, message, Optional.empty());

    verify(logger).error(message);
    verifyNoMoreInteractions(logger);
  }

  @ParameterizedTest
  @EnumSource(
      value = Level.class,
      names = {"WARN", "INFO", "DEBUG", "TRACE"})
  void testNonErrorLevels(Level level) {
    String message = "Test message";

    LogHelper.logAtLevel(logger, level, message, Optional.empty());

    if (level == Level.WARN) {
      verify(logger).warn(message);
    } else if (level == Level.INFO) {
      verify(logger).info(message);
    } else if (level == Level.DEBUG) {
      verify(logger).debug(message);
    } else if (level == Level.TRACE) {
      verify(logger).trace(message);
    } else {
      fail("Unexpected log level: " + level);
    }

    verifyNoMoreInteractions(logger);
  }

  @Test
  void testNonErrorLevelsWithException() {
    String message = "Test message";
    Exception exception = new RuntimeException("Test exception");

    // Exception should be ignored for non-ERROR levels
    LogHelper.logAtLevel(logger, Level.WARN, message, Optional.of(exception));
    LogHelper.logAtLevel(logger, Level.INFO, message, Optional.of(exception));
    LogHelper.logAtLevel(logger, Level.DEBUG, message, Optional.of(exception));
    LogHelper.logAtLevel(logger, Level.TRACE, message, Optional.of(exception));

    verify(logger).warn(message);
    verify(logger).info(message);
    verify(logger).debug(message);
    verify(logger).trace(message);
    verifyNoMoreInteractions(logger);
  }

  @Test
  void testNullMessage() {
    LogHelper.logAtLevel(logger, Level.INFO, null, Optional.empty());

    verify(logger).info(null);
    verifyNoMoreInteractions(logger);
  }

  @Test
  void testEmptyMessage() {
    LogHelper.logAtLevel(logger, Level.INFO, "", Optional.empty());

    verify(logger).info("");
    verifyNoMoreInteractions(logger);
  }

  @Test
  void testLongMessage() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 1000; i++) {
      sb.append("a");
    }
    String longMessage = sb.toString();

    LogHelper.logAtLevel(logger, Level.INFO, longMessage, Optional.empty());

    verify(logger).info(longMessage);
    verifyNoMoreInteractions(logger);
  }

  @Test
  void testWithNullException() {
    String message = "Test message";

    LogHelper.logAtLevel(logger, Level.ERROR, message, Optional.ofNullable(null));

    verify(logger).error(message);
    verifyNoMoreInteractions(logger);
  }
}
