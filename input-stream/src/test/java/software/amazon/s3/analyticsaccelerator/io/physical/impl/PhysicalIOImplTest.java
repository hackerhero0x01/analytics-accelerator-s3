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
package software.amazon.s3.analyticsaccelerator.io.physical.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.TestTelemetry;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.data.BlobStore;
import software.amazon.s3.analyticsaccelerator.io.physical.data.MetadataStore;
import software.amazon.s3.analyticsaccelerator.request.StreamContext;
import software.amazon.s3.analyticsaccelerator.util.FakeObjectClient;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class PhysicalIOImplTest {

  private static final S3URI s3URI = S3URI.of("foo", "bar");

  @Test
  void testConstructorThrowsOnNullArgument() {
    assertThrows(
        NullPointerException.class,
        () -> {
          new PhysicalIOImpl(
              s3URI, null, mock(BlobStore.class), TestTelemetry.DEFAULT, mock(StreamContext.class));
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          new PhysicalIOImpl(
              s3URI,
              mock(MetadataStore.class),
              null,
              TestTelemetry.DEFAULT,
              mock(StreamContext.class));
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          new PhysicalIOImpl(
              null, mock(MetadataStore.class), mock(BlobStore.class), TestTelemetry.DEFAULT);
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          new PhysicalIOImpl(s3URI, mock(MetadataStore.class), mock(BlobStore.class), null);
        });
    assertThrows(
        NullPointerException.class,
        () -> {
          new PhysicalIOImpl(s3URI, null, mock(BlobStore.class), TestTelemetry.DEFAULT);
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          new PhysicalIOImpl(s3URI, mock(MetadataStore.class), null, TestTelemetry.DEFAULT);
        });
    assertThrows(
        NullPointerException.class,
        () -> {
          new PhysicalIOImpl(
              null, mock(MetadataStore.class), mock(BlobStore.class), TestTelemetry.DEFAULT);
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          new PhysicalIOImpl(s3URI, mock(MetadataStore.class), mock(BlobStore.class), null);
        });
  }

  @Test
  public void test__readSingleByte_isCorrect() throws IOException {
    // Given: physicalIOImplV2
    final String TEST_DATA = "abcdef0123456789";
    FakeObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    MetadataStore metadataStore =
        new MetadataStore(fakeObjectClient, TestTelemetry.DEFAULT, PhysicalIOConfiguration.DEFAULT);
    BlobStore blobStore =
        new BlobStore(
            metadataStore,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT);
    PhysicalIOImpl physicalIOImplV2 =
        new PhysicalIOImpl(s3URI, metadataStore, blobStore, TestTelemetry.DEFAULT);

    // When: we read
    // Then: returned data is correct
    assertEquals(97, physicalIOImplV2.read(0)); // a
    assertEquals(98, physicalIOImplV2.read(1)); // b
    assertEquals(99, physicalIOImplV2.read(2)); // c
  }

  @Test
  public void test__regression_singleByteStream() throws IOException {
    // Given: physicalIOImplV2 backed by a single byte object
    final String TEST_DATA = "x";
    FakeObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    MetadataStore metadataStore =
        new MetadataStore(fakeObjectClient, TestTelemetry.DEFAULT, PhysicalIOConfiguration.DEFAULT);
    BlobStore blobStore =
        new BlobStore(
            metadataStore,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT);
    PhysicalIOImpl physicalIOImplV2 =
        new PhysicalIOImpl(s3URI, metadataStore, blobStore, TestTelemetry.DEFAULT);

    // When: we read
    // Then: returned data is correct
    assertEquals(120, physicalIOImplV2.read(0)); // a
  }

  @Test
  void testReadWithBuffer() throws IOException {
    final String TEST_DATA = "abcdef0123456789";
    FakeObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    MetadataStore metadataStore =
        new MetadataStore(fakeObjectClient, TestTelemetry.DEFAULT, PhysicalIOConfiguration.DEFAULT);
    BlobStore blobStore =
        new BlobStore(
            metadataStore,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT);
    PhysicalIOImpl physicalIOImplV2 =
        new PhysicalIOImpl(s3URI, metadataStore, blobStore, TestTelemetry.DEFAULT);

    byte[] buffer = new byte[5];
    assertEquals(5, physicalIOImplV2.read(buffer, 0, 5, 5));
  }

  @Test
  void testReadTail() throws IOException {
    final String TEST_DATA = "abcdef0123456789";
    FakeObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    MetadataStore metadataStore =
        new MetadataStore(fakeObjectClient, TestTelemetry.DEFAULT, PhysicalIOConfiguration.DEFAULT);
    BlobStore blobStore =
        new BlobStore(
            metadataStore,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT);
    PhysicalIOImpl physicalIOImplV2 =
        new PhysicalIOImpl(s3URI, metadataStore, blobStore, TestTelemetry.DEFAULT);
    byte[] buffer = new byte[5];
    assertEquals(5, physicalIOImplV2.readTail(buffer, 0, 5));
  }
}
