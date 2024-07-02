package com.amazon.connector.s3.io.logical.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.object.ObjectMetadata;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

public class ParquetReadTailTaskTest {

  @Test
  void testConstructor() {
    assertNotNull(new ParquetReadTailTask(LogicalIOConfiguration.DEFAULT, mock(PhysicalIO.class)));
  }

  @Test
  void testConstructorFailsOnNull() {
    assertThrows(
        NullPointerException.class, () -> new ParquetReadTailTask(null, mock(PhysicalIO.class)));
    assertThrows(
        NullPointerException.class,
        () -> new ParquetReadTailTask(LogicalIOConfiguration.DEFAULT, null));
  }

  @Test
  void testTailRead() throws IOException {
    // Given: read tail task
    PhysicalIO mockedPhysicalIO = mock(PhysicalIO.class);
    when(mockedPhysicalIO.metadata())
        .thenReturn(
            CompletableFuture.completedFuture(ObjectMetadata.builder().contentLength(800).build()));
    ParquetReadTailTask parquetReadTailTask =
        new ParquetReadTailTask(LogicalIOConfiguration.DEFAULT, mockedPhysicalIO);

    // When: file tail is requested
    FileTail fileTail = parquetReadTailTask.readFileTail();

    // Then: file tail is fetched from the PhysicalIO
    assertEquals(fileTail.getFileTailLength(), 800);
    verify(mockedPhysicalIO).readTail(any(byte[].class), anyInt(), anyInt());
    verify(mockedPhysicalIO).metadata();
  }

  @Test
  @SneakyThrows
  void testExceptionRemappedToCompletionException() {
    // Given: read tail task with a throwing physicalIO under the hood
    PhysicalIO mockedPhysicalIO = mock(PhysicalIO.class);
    when(mockedPhysicalIO.metadata())
        .thenReturn(
            CompletableFuture.completedFuture(ObjectMetadata.builder().contentLength(800).build()));
    when(mockedPhysicalIO.readTail(any(), anyInt(), anyInt()))
        .thenThrow(new IOException("Something went horribly wrong."));
    ParquetReadTailTask parquetReadTailTask =
        new ParquetReadTailTask(LogicalIOConfiguration.DEFAULT, mockedPhysicalIO);

    // When & Then: file tail is requested --> IOException from PH/IO is wrapped in
    // CompletionException
    assertThrows(CompletionException.class, () -> parquetReadTailTask.readFileTail());
  }
}
