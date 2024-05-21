package com.amazon.connector.s3.io.physical.blockmanager;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import org.junit.jupiter.api.Test;

public class BlockManagerTest {

  private static final S3URI URI = S3URI.of("bucket", "key");

  @Test
  void testConstructor() {
    // When: constructor is called
    BlockManager blockManager =
        new BlockManager(mock(ObjectClient.class), URI, BlockManagerConfiguration.DEFAULT);

    // Then: result is not null
    assertNotNull(blockManager);
  }

  @Test
  void testConstructorFailsOnNull() {
    assertThrows(
        NullPointerException.class,
        () -> new BlockManager(null, URI, BlockManagerConfiguration.DEFAULT));
    assertThrows(
        NullPointerException.class,
        () -> new BlockManager(mock(ObjectClient.class), null, BlockManagerConfiguration.DEFAULT));
    assertThrows(
        NullPointerException.class, () -> new BlockManager(mock(ObjectClient.class), URI, null));
  }

  @Test
  void testClose() throws IOException {
    // Given: object client
    ObjectClient objectClient = mock(ObjectClient.class);
    BlockManager blockManager =
        new BlockManager(objectClient, URI, BlockManagerConfiguration.DEFAULT);

    // When: close is called
    blockManager.close();

    // Object client is not closed, as we want to share the client b/w streams.
    verify(objectClient, times(0)).close();
  }
}
