package com.amazon.connector.s3.io.logical.impl;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.amazon.connector.s3.io.physical.PhysicalIO;
import java.io.IOException;
import org.junit.jupiter.api.Test;

public class LogicalIOImplTest {

  @Test
  void testContructor() {
    assertNotNull(new LogicalIOImpl(mock(PhysicalIO.class)));
  }

  @Test
  void testCloseDependencies() throws IOException {
    // Given
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    LogicalIOImpl logicalIO = new LogicalIOImpl(physicalIO);

    // When: close called
    logicalIO.close();

    // Then: close will close dependencies
    verify(physicalIO, times(1)).close();
  }
}
