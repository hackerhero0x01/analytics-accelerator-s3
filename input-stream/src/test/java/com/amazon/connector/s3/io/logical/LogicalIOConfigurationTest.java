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
package com.amazon.connector.s3.io.logical;

import static org.junit.jupiter.api.Assertions.*;

import com.amazon.connector.s3.S3SeekableInputStreamConfiguration;
import com.amazon.connector.s3.S3SeekableInputStreamConfigurationTest;
import com.amazon.connector.s3.common.ConnectorConfiguration;
import com.amazon.connector.s3.util.PrefetchMode;
import org.junit.jupiter.api.Test;

public class LogicalIOConfigurationTest {

  @Test
  void testDefaultBuilder() {
    LogicalIOConfiguration configuration = LogicalIOConfiguration.builder().build();
    assertEquals(LogicalIOConfiguration.DEFAULT, configuration);
  }

  @Test
  void testNonDefaults() {
    LogicalIOConfiguration configuration =
        LogicalIOConfiguration.builder().footerCachingEnabled(true).footerCachingSize(10).build();
    assertTrue(configuration.isFooterCachingEnabled());
    assertEquals(10, configuration.getFooterCachingSize());
  }

  @Test
  void testFromConfiguration() {
    ConnectorConfiguration configuration =
        S3SeekableInputStreamConfigurationTest.getConfiguration();
    ConnectorConfiguration mappedConfiguration =
        configuration.map(S3SeekableInputStreamConfiguration.LOGICAL_IO_PREFIX);
    LogicalIOConfiguration logicalIOConfiguration =
        LogicalIOConfiguration.fromConfiguration(mappedConfiguration);

    assertFalse(logicalIOConfiguration.isFooterCachingEnabled());
    assertEquals(20, logicalIOConfiguration.getFooterCachingSize());
    // This should be equal to Default since Property Prefix is not s3.connector.
    assertEquals(
        LogicalIOConfiguration.DEFAULT.getPrefetchingMode(),
        logicalIOConfiguration.getPrefetchingMode());
    assertEquals(logicalIOConfiguration.getPrefetchingMode(), PrefetchMode.ROW_GROUP);
  }
}
