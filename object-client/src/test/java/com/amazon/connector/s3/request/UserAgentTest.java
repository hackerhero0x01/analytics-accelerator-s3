  /*
   * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
   *
   * Licensed under the Apache License, Version 2.0 (the "License").
   * You may not use this file except in compliance with the License.
   * You may obtain a copy of the License at
   *
   *   http://www.apache.org/licenses/LICENSE-2.0
  
   * Unless required by applicable law or agreed to in writing, software
   * distributed under the License is distributed on an "AS IS" BASIS,
   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   * See the License for the specific language governing permissions and
   * limitations under the License.
   */
package com.amazon.connector.s3.request;

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
