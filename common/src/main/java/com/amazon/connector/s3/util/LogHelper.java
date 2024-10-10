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
package com.amazon.connector.s3.util;

import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.event.Level;

/** A helper class for logging related methods. */
public class LogHelper {

  /**
   * Logs at the specified level.
   *
   * @param logger logger to use
   * @param level level to log at all
   * @param message message to log
   * @param error optional error to log
   */
  public static void logAtLevel(
      Logger logger, Level level, String message, Optional<Throwable> error) {
    switch (level) {
      case ERROR:
        if (error.isPresent()) {
          logger.error(message, error.get());
        } else {
          logger.error(message);
        }
        break;
      case WARN:
        logger.warn(message);
        break;
      case INFO:
        logger.info(message);
        break;
      case DEBUG:
        logger.debug(message);
        break;
      case TRACE:
        logger.trace(message);
        break;
    }
  }
}
