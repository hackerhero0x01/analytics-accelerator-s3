package com.amazon.connector.s3.io.physical.impl;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.io.physical.blockmanager.BlockManager;
import com.amazon.connector.s3.io.physical.blockmanager.BlockManagerConfiguration;
import com.amazon.connector.s3.io.physical.plan.IOPlan;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import org.junit.jupiter.api.Test;

public class PhysicalIOImplTest {

  @Test
  void testConstructor() {
    PhysicalIOImpl physicalIO =
        new PhysicalIOImpl(
            mock(ObjectClient.class), S3URI.of("a", "b"), BlockManagerConfiguration.DEFAULT);
    assertNotNull(physicalIO);
  }

  @Test
  void testExecuteNotImplementedThrows() {
    PhysicalIOImpl physicalIO =
        new PhysicalIOImpl(
            mock(ObjectClient.class), S3URI.of("a", "b"), BlockManagerConfiguration.DEFAULT);

    assertThrows(UnsupportedOperationException.class, () -> physicalIO.execute(new IOPlan()));
  }

  @Test
  void testCloseDependencies() throws IOException {
    // Given
    BlockManager blockManager = mock(BlockManager.class);
    PhysicalIOImpl physicalIO = new PhysicalIOImpl(blockManager);

    // When
    physicalIO.close();

    // Then
    verify(blockManager, times(1)).close();
  }
}
