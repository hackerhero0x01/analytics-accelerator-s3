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

import java.io.IOException;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.TestTelemetry;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.data.BlobStore;
import software.amazon.s3.analyticsaccelerator.io.physical.data.MetadataStore;
import software.amazon.s3.analyticsaccelerator.util.FakeObjectClient;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

public class PhysicalIOImplTest {

  private static final S3URI s3URI = S3URI.of("foo", "bar");

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
}
