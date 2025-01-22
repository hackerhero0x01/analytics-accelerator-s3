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
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.StreamContext;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class S3SeekableInputStreamFactoryTest {
  private static S3URI s3URI = S3URI.of("bucket", "key");
  private static int CONTENT_LENGTH = 500;
  private static final ObjectMetadata objectMetadata =
      ObjectMetadata.builder().contentLength(CONTENT_LENGTH).etag(Optional.of("ETAG")).build();

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

    s3SeekableInputStreamFactory
        .getObjectMetadataStore()
        .storeObjectMetadata(s3URI, objectMetadata);
    S3SeekableInputStream inputStream =
        s3SeekableInputStreamFactory.createStream(S3URI.of("bucket", "key"));
    assertNotNull(inputStream);

    inputStream =
        s3SeekableInputStreamFactory.createStream(
            S3URI.of("bucket", "key"), mock(StreamContext.class));
    assertNotNull(inputStream);
  }

  @Test
  void testCreateStreamWithContentLengthAndEtag() {
    S3SeekableInputStreamFactory s3SeekableInputStreamFactory =
        new S3SeekableInputStreamFactory(
            mock(ObjectClient.class),
            S3SeekableInputStreamConfiguration.builder()
                .logicalIOConfiguration(
                    LogicalIOConfiguration.builder().prefetchFooterEnabled(false).build())
                .build());
    S3SeekableInputStream inputStream =
        s3SeekableInputStreamFactory.createStream(s3URI, objectMetadata);
    assertNotNull(inputStream);
    assertEquals(
        CONTENT_LENGTH,
        s3SeekableInputStreamFactory.getObjectMetadataStore().get(s3URI).getContentLength());
    assertEquals(
        objectMetadata.getEtag().get(),
        s3SeekableInputStreamFactory.getObjectMetadataStore().get(s3URI).getEtag().get());
  }

  @Test
  void testCreateStreamWithJustContentLength() {
    S3SeekableInputStreamFactory s3SeekableInputStreamFactory =
        new S3SeekableInputStreamFactory(
            mock(ObjectClient.class),
            S3SeekableInputStreamConfiguration.builder()
                .logicalIOConfiguration(
                    LogicalIOConfiguration.builder().prefetchFooterEnabled(false).build())
                .build());
    S3SeekableInputStream inputStream =
        s3SeekableInputStreamFactory.createStream(
            s3URI,
            ObjectMetadata.builder().contentLength(CONTENT_LENGTH).etag(Optional.empty()).build());
    assertNotNull(inputStream);
    assertEquals(
        CONTENT_LENGTH,
        s3SeekableInputStreamFactory.getObjectMetadataStore().get(s3URI).getContentLength());
    assertFalse(
        s3SeekableInputStreamFactory.getObjectMetadataStore().get(s3URI).getEtag().isPresent());
  }

  @Test
  void testPreconditions() {
    S3SeekableInputStreamFactory s3SeekableInputStreamFactory =
        new S3SeekableInputStreamFactory(
            mock(ObjectClient.class),
            S3SeekableInputStreamConfiguration.builder()
                .logicalIOConfiguration(
                    LogicalIOConfiguration.builder().prefetchFooterEnabled(false).build())
                .build());

    assertThrows(
        Exception.class,
        () ->
            s3SeekableInputStreamFactory.createStream(
                s3URI, ObjectMetadata.builder().contentLength(-1).build()));
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
    s3SeekableInputStreamFactory
        .getObjectMetadataStore()
        .storeObjectMetadata(s3URI, objectMetadata);

    S3SeekableInputStream inputStream = s3SeekableInputStreamFactory.createStream(s3URI);
    assertNotNull(inputStream);

    inputStream = s3SeekableInputStreamFactory.createStream(s3URI, mock(StreamContext.class));
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

    assertThrows(
        NullPointerException.class,
        () -> {
          s3SeekableInputStreamFactory.createStream(null, mock(StreamContext.class));
        });
  }

  @Test
  void testCreateLogicalIO() {
    S3URI testURIParquet = S3URI.of("bucket", "key.parquet");
    S3URI testURIKEYPAR = S3URI.of("bucket", "key.par");
    S3URI testURIJAVA = S3URI.of("bucket", "key.java");
    S3URI testURITXT = S3URI.of("bucket", "key.txt");
    S3SeekableInputStreamConfiguration configuration =
        S3SeekableInputStreamConfiguration.builder()
            .logicalIOConfiguration(
                LogicalIOConfiguration.builder().prefetchFooterEnabled(false).build())
            .build();
    S3SeekableInputStreamFactory s3SeekableInputStreamFactory =
        new S3SeekableInputStreamFactory(mock(ObjectClient.class), configuration);
    s3SeekableInputStreamFactory
        .getObjectMetadataStore()
        .storeObjectMetadata(testURIParquet, objectMetadata);
    s3SeekableInputStreamFactory
        .getObjectMetadataStore()
        .storeObjectMetadata(testURIKEYPAR, objectMetadata);
    s3SeekableInputStreamFactory
        .getObjectMetadataStore()
        .storeObjectMetadata(testURIJAVA, objectMetadata);
    s3SeekableInputStreamFactory
        .getObjectMetadataStore()
        .storeObjectMetadata(testURITXT, objectMetadata);

    assertTrue(
        s3SeekableInputStreamFactory.createLogicalIO(testURIParquet, mock(StreamContext.class))
            instanceof ParquetLogicalIOImpl);
    assertTrue(
        s3SeekableInputStreamFactory.createLogicalIO(testURIKEYPAR, mock(StreamContext.class))
            instanceof ParquetLogicalIOImpl);

    assertTrue(
        s3SeekableInputStreamFactory.createLogicalIO(testURIJAVA, mock(StreamContext.class))
            instanceof DefaultLogicalIOImpl);
    assertTrue(
        s3SeekableInputStreamFactory.createLogicalIO(testURITXT, mock(StreamContext.class))
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
