package com.amazon.connector.s3;

import java.util.Objects;
import lombok.Getter;

/** User-Agent to be used by ObjectClients */
@Getter
public final class UserAgent {
  // Hard-coded user-agent string
  private static final String UA_STRING = "s3connectorframework";
  // TODO: Get VersionInfo and append it to UA. We need to understand how to create (if we want a)
  // global version (for InputStream and ObjectClient).
  private static final String VERSION_INFO = null;
  /**
   * Disallowed characters in the user agent token: @see <a
   * href="https://tools.ietf.org/html/rfc7230#section-3.2.6">RFC 7230</a>
   */
  private static final String UA_DENYLIST_REGEX = "[() ,/:;<=>?@\\[\\]{}\\\\]";

  private String userAgent = UA_STRING + ":" + VERSION_INFO;

  /**
   * Prepend hard-coded user-agent string with input string provided.
   *
   * @param userAgentPrefix to prepend the default user-agent string
   */
  public void prepend(String userAgentPrefix) {
    if (Objects.nonNull(userAgentPrefix))
      this.userAgent = sanitizeInput(userAgentPrefix) + "/" + this.userAgent;
  }

  private static String sanitizeInput(String input) {
    return input == null ? "unknown" : input.replaceAll(UA_DENYLIST_REGEX, "_");
  }
}
