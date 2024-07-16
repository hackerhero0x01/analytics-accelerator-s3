package com.amazon.connector.s3.io.physical.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazon.connector.s3.io.physical.v2.data.BlobStore;
import com.amazon.connector.s3.io.physical.v2.data.MetadataStore;
import com.amazon.connector.s3.util.FakeObjectClient;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import org.junit.jupiter.api.Test;

public class PhysicalIOImplV2Test {

  private static final S3URI s3URI = S3URI.of("foo", "bar");

  @Test
  public void test__readSingleByte_isCorrect() throws IOException {
    // Given: physicalIOImplV2
    final String TEST_DATA = "abcdef0123456789";
    FakeObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    MetadataStore metadataStore = new MetadataStore(fakeObjectClient);
    BlobStore blobStore = new BlobStore(metadataStore, fakeObjectClient);
    PhysicalIOImplV2 physicalIOImplV2 = new PhysicalIOImplV2(s3URI, metadataStore, blobStore);

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
    MetadataStore metadataStore = new MetadataStore(fakeObjectClient);
    BlobStore blobStore = new BlobStore(metadataStore, fakeObjectClient);
    PhysicalIOImplV2 physicalIOImplV2 = new PhysicalIOImplV2(s3URI, metadataStore, blobStore);

    // When: we read
    // Then: returned data is correct
    assertEquals(120, physicalIOImplV2.read(0)); // a
  }
}
