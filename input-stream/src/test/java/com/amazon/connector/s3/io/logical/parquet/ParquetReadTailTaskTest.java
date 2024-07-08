package com.amazon.connector.s3.io.logical.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

public class ParquetReadTailTaskTest {

  @Test
  void testContructor() {
    assertNotNull(new ParquetReadTailTask(LogicalIOConfiguration.DEFAULT, mock(PhysicalIO.class)));
  }

  @Test
  void testContructorFailsOnNull() {
    assertThrows(
        NullPointerException.class, () -> new ParquetReadTailTask(null, mock(PhysicalIO.class)));
    assertThrows(
        NullPointerException.class,
        () -> new ParquetReadTailTask(LogicalIOConfiguration.DEFAULT, null));
  }

  @Test
  void testTailRead() throws IOException {
    PhysicalIO mockedPhysicalIO = mock(PhysicalIO.class);
    when(mockedPhysicalIO.metadata())
        .thenReturn(
            CompletableFuture.completedFuture(ObjectMetadata.builder().contentLength(800).build()));
    ParquetReadTailTask parquetReadTailTask =
        new ParquetReadTailTask(LogicalIOConfiguration.DEFAULT, mockedPhysicalIO);

    Optional<FileTail> fileTail = parquetReadTailTask.readFileTail();

    assertTrue(fileTail.isPresent());
    assertEquals(fileTail.get().getFileTailLength(), 800);
    verify(mockedPhysicalIO).readTail(any(byte[].class), anyInt(), anyInt());
    verify(mockedPhysicalIO).metadata();
  }

  @Test
  void testExceptionSwallowed() throws IOException {
    PhysicalIO mockedPhysicalIO = mock(PhysicalIO.class);
    when(mockedPhysicalIO.getS3URI()).thenReturn(S3URI.of("test", "data"));
    ParquetReadTailTask parquetReadTailTask =
        new ParquetReadTailTask(LogicalIOConfiguration.DEFAULT, mockedPhysicalIO);
    when(mockedPhysicalIO.metadata())
        .thenReturn(
            CompletableFuture.completedFuture(ObjectMetadata.builder().contentLength(800).build()));
    when(mockedPhysicalIO.readTail(any(byte[].class), anyInt(), anyInt()))
        .thenThrow(new IOException("Error in reading tail"));

    assertFalse(parquetReadTailTask.readFileTail().isPresent());
  }
}
