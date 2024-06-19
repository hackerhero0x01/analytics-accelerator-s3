package com.amazon.connector.s3.io.physical.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.io.logical.parquet.ColumnMappers;
import com.amazon.connector.s3.io.physical.blockmanager.BlockManager;
import com.amazon.connector.s3.io.physical.blockmanager.BlockManagerConfiguration;
import com.amazon.connector.s3.io.physical.plan.IOPlan;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
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
  void testDependentConstructor() {
    PhysicalIOImpl physicalIO = new PhysicalIOImpl(mock(BlockManager.class));
    assertNotNull(physicalIO);
  }

  @Test
  void testExecuteNotImplementedThrows() {
    PhysicalIOImpl physicalIO =
        new PhysicalIOImpl(
            mock(ObjectClient.class), S3URI.of("a", "b"), BlockManagerConfiguration.DEFAULT);

    assertThrows(
        InvalidParameterException.class, () -> physicalIO.execute(IOPlan.builder().build()));
  }

  @Test
  void testPutColumnMappers() {
    BlockManager blockManager = mock(BlockManager.class);
    PhysicalIOImpl physicalIO = new PhysicalIOImpl(blockManager);
    physicalIO.putColumnMappers(new ColumnMappers(new HashMap<>(), new HashMap<>()));
    verify(blockManager).putColumnMappers(any(ColumnMappers.class));
  }

  @Test
  void testAddRecentColumns() {
    BlockManager blockManager = mock(BlockManager.class);
    PhysicalIOImpl physicalIO = new PhysicalIOImpl(blockManager);
    physicalIO.addRecentColumn("sk_test");
    verify(blockManager).addRecentColumn("sk_test");
  }

  @Test
  void testGetRecentColumns() {
    BlockManager blockManager = mock(BlockManager.class);
    Set<String> recentColumns = new HashSet<>();
    recentColumns.add("sk_test");
    when(blockManager.getRecentColumns()).thenReturn(recentColumns);
    PhysicalIOImpl physicalIO = new PhysicalIOImpl(blockManager);
    assertEquals(recentColumns, physicalIO.getRecentColumns());
    verify(blockManager).getRecentColumns();
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
