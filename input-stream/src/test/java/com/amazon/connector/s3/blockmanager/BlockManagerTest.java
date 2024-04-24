package com.amazon.connector.s3.blockmanager;

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
    BlockManager blockManager = new BlockManager(mock(ObjectClient.class), URI);

    // Then: result is not null
    assertNotNull(blockManager);
  }

  @Test
  void testConstructorFailsOnNull() {
    assertThrows(NullPointerException.class, () -> new BlockManager(null, URI));
    assertThrows(
        NullPointerException.class, () -> new BlockManager(mock(ObjectClient.class), null));
  }

  @Test
  void testClose() throws IOException {
    // Given: object client
    ObjectClient objectClient = mock(ObjectClient.class);
    BlockManager blockManager = new BlockManager(objectClient, URI);

    // When: close is called
    blockManager.close();

    // Then: objectClient is also closed
    verify(objectClient, times(1)).close();
  }
}
