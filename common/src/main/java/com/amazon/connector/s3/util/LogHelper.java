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
