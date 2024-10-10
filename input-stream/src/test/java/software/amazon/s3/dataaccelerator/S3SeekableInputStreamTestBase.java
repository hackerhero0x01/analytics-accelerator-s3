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
package software.amazon.s3.dataaccelerator;

import software.amazon.s3.dataaccelerator.io.logical.LogicalIO;
import software.amazon.s3.dataaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.dataaccelerator.io.logical.impl.ParquetColumnPrefetchStore;
import software.amazon.s3.dataaccelerator.io.logical.impl.ParquetLogicalIOImpl;
import software.amazon.s3.dataaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.dataaccelerator.io.physical.data.BlobStore;
import software.amazon.s3.dataaccelerator.io.physical.data.MetadataStore;
import software.amazon.s3.dataaccelerator.io.physical.impl.PhysicalIOImpl;
import software.amazon.s3.dataaccelerator.util.FakeObjectClient;
import software.amazon.s3.dataaccelerator.util.S3URI;

public class S3SeekableInputStreamTestBase {

  protected static final String TEST_DATA = "test-data12345678910";
  protected static final S3URI TEST_OBJECT = S3URI.of("bucket", "key");

  protected final PhysicalIOConfiguration physicalIOConfiguration = PhysicalIOConfiguration.DEFAULT;
  protected final FakeObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
  protected final MetadataStore metadataStore =
      new MetadataStore(fakeObjectClient, TestTelemetry.DEFAULT, PhysicalIOConfiguration.DEFAULT);
  protected final BlobStore blobStore =
      new BlobStore(
          metadataStore, fakeObjectClient, TestTelemetry.DEFAULT, physicalIOConfiguration);
  protected final LogicalIOConfiguration logicalIOConfiguration = LogicalIOConfiguration.DEFAULT;

  protected final LogicalIO fakeLogicalIO =
      new ParquetLogicalIOImpl(
          TEST_OBJECT,
          new PhysicalIOImpl(TEST_OBJECT, metadataStore, blobStore, TestTelemetry.DEFAULT),
          TestTelemetry.DEFAULT,
          logicalIOConfiguration,
          new ParquetColumnPrefetchStore(logicalIOConfiguration));
}
