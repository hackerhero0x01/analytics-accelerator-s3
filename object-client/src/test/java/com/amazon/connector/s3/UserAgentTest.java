package com.amazon.connector.s3;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class UserAgentTest {

  @Test
  void testDefaultUserAgent() {
    UserAgent agent = new UserAgent();
    assertNotNull(agent.getUserAgent());
  }

  @Test
  void testPrependUserAgent() {
    UserAgent agent = new UserAgent();
    agent.prepend("unit_test");
    assertTrue(agent.getUserAgent().startsWith("unit_test"));
  }

  @Test
  void testNullPrependIsNoop() {
    UserAgent agent = new UserAgent();
    String pre = agent.getUserAgent();
    agent.prepend(null);
    String pos = agent.getUserAgent();
    assertEquals(pre, pos);
  }

  @Test
  void testInvalidInputIsSanitised() {
    UserAgent agent = new UserAgent();
    agent.prepend("unit/test");
    assertTrue(agent.getUserAgent().startsWith("unit_test"));
  }
}
