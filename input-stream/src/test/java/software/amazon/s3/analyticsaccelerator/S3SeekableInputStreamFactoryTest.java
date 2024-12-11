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
package software.amazon.s3.analyticsaccelerator;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.logical.impl.DefaultLogicalIOImpl;
import software.amazon.s3.analyticsaccelerator.io.logical.impl.ParquetLogicalIOImpl;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class S3SeekableInputStreamFactoryTest {
  @Test
  void testConstructor() {
    ObjectClient objectClient = mock(ObjectClient.class);
    S3SeekableInputStreamFactory s3SeekableInputStreamFactory =
        new S3SeekableInputStreamFactory(objectClient, S3SeekableInputStreamConfiguration.DEFAULT);
    assertEquals(
        S3SeekableInputStreamConfiguration.DEFAULT,
        s3SeekableInputStreamFactory.getConfiguration());
    assertEquals(objectClient, s3SeekableInputStreamFactory.getObjectClient());
  }

  @Test
  void testConstructorThrowsOnNullArgument() {
    assertThrows(
        NullPointerException.class,
        () -> {
          new S3SeekableInputStreamFactory(null, S3SeekableInputStreamConfiguration.DEFAULT);
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          new S3SeekableInputStreamFactory(mock(ObjectClient.class), null);
        });
  }

  @Test
  void testCreateDefaultStream() {
    S3SeekableInputStreamFactory s3SeekableInputStreamFactory =
        new S3SeekableInputStreamFactory(
            mock(ObjectClient.class),
            S3SeekableInputStreamConfiguration.builder()
                .logicalIOConfiguration(
                    LogicalIOConfiguration.builder().prefetchFooterEnabled(false).build())
                .build());
    S3SeekableInputStream inputStream =
        s3SeekableInputStreamFactory.createStream(S3URI.of("bucket", "key"));
    assertNotNull(inputStream);
  }

  @Test
  void testCreateIndependentStream() {
    S3SeekableInputStreamConfiguration configuration =
        S3SeekableInputStreamConfiguration.builder()
            .logicalIOConfiguration(
                LogicalIOConfiguration.builder().prefetchFooterEnabled(false).build())
            .build();
    S3SeekableInputStreamFactory s3SeekableInputStreamFactory =
        new S3SeekableInputStreamFactory(mock(ObjectClient.class), configuration);
    S3SeekableInputStream inputStream =
        s3SeekableInputStreamFactory.createStream(S3URI.of("bucket", "key"));
    assertNotNull(inputStream);
  }

  @Test
  void testCreateStreamThrowsOnNullArgument() {
    S3SeekableInputStreamFactory s3SeekableInputStreamFactory =
        new S3SeekableInputStreamFactory(
            mock(ObjectClient.class), S3SeekableInputStreamConfiguration.DEFAULT);
    assertThrows(
        NullPointerException.class,
        () -> {
          s3SeekableInputStreamFactory.createStream(null);
        });
  }

  @Test
  void testCreateLogicalIO() {
    S3SeekableInputStreamConfiguration configuration =
        S3SeekableInputStreamConfiguration.builder()
            .logicalIOConfiguration(
                LogicalIOConfiguration.builder().prefetchFooterEnabled(false).build())
            .build();
    S3SeekableInputStreamFactory s3SeekableInputStreamFactory =
        new S3SeekableInputStreamFactory(mock(ObjectClient.class), configuration);

    assertTrue(
        s3SeekableInputStreamFactory.createLogicalIO(
                S3URI.of("bucket", "key.parquet"), Optional.empty())
            instanceof ParquetLogicalIOImpl);
    assertTrue(
        s3SeekableInputStreamFactory.createLogicalIO(
                S3URI.of("bucket", "key.par"), Optional.empty())
            instanceof ParquetLogicalIOImpl);

    assertTrue(
        s3SeekableInputStreamFactory.createLogicalIO(
                S3URI.of("bucket", "key.java"), Optional.empty())
            instanceof DefaultLogicalIOImpl);
    assertTrue(
        s3SeekableInputStreamFactory.createLogicalIO(
                S3URI.of("bucket", "key.txt"), Optional.empty())
            instanceof DefaultLogicalIOImpl);
  }

  @Test
  void testClose() {
    S3SeekableInputStreamFactory s3SeekableInputStreamFactory =
        new S3SeekableInputStreamFactory(
            mock(ObjectClient.class), S3SeekableInputStreamConfiguration.DEFAULT);
    assertDoesNotThrow(() -> s3SeekableInputStreamFactory.close());
  }
}
