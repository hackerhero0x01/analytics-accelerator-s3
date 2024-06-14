package com.amazon.connector.s3.io.physical.blockmanager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

public class AutoClosingCircularBufferTest {

  @Test
  void testConstructor() {
    assertThrows(Exception.class, () -> new AutoClosingCircularBuffer(-1));
    assertThrows(Exception.class, () -> new AutoClosingCircularBuffer(0));
  }

  @Test
  void testEvictionClosesResource() throws IOException {
    // Given
    AutoClosingCircularBuffer circularBuffer = new AutoClosingCircularBuffer(2);
    Closeable c1 = mock(Closeable.class);
    Closeable c2 = mock(Closeable.class);
    Closeable c3 = mock(Closeable.class);

    // When: elements are added
    circularBuffer.add(c1);
    circularBuffer.add(c2);
    circularBuffer.add(c3);

    // Then: oldest element (c1) is closed, but other elements are not closed
    verify(c1, times(1)).close();
    verify(c2, times(0)).close();
    verify(c3, times(0)).close();
  }

  @Test
  void testClose() throws IOException {
    // Given
    AutoClosingCircularBuffer circularBuffer = new AutoClosingCircularBuffer(2);
    Closeable c1 = mock(Closeable.class);
    Closeable c2 = mock(Closeable.class);
    Closeable c3 = mock(Closeable.class);

    // When: elements are added and the buffer closed
    circularBuffer.add(c1);
    circularBuffer.add(c2);
    circularBuffer.add(c3);
    circularBuffer.close();

    // Then: all elements are closed
    for (Closeable c : Arrays.asList(c1, c2, c3)) {
      verify(c, times(1)).close();
    }

    // assertThrows(IllegalArgumentException.class, () ->
    // circularBuffer.add(mock(Closeable.class)));
    // assertThrows(IllegalArgumentException.class, () -> circularBuffer.stream());
  }

  @Test
  void testStream() throws IOException {
    // Given
    AutoClosingCircularBuffer circularBuffer = new AutoClosingCircularBuffer(2);

    // When: 3 elements are added and then buffer is converted to a stream
    for (Closeable c :
        Arrays.asList(mock(Closeable.class), mock(Closeable.class), mock(Closeable.class))) {
      circularBuffer.add(c);
    }
    Stream<Closeable> stream = circularBuffer.stream();

    // Then: stream has only maxCapacity elements
    assertEquals(2, stream.count());
  }

  @Test
  // This is contentious: I wasn't completely sure what's the best choice here, but for now, making
  // 'add' fail is a stricter handling of this situation than just ignoring problems. We can decide
  // later if this is the wrong choice, but we shouldn't start with 'ignore' by default.
  void testFailureInClosePreventsAddToSucceed() throws IOException {
    // Given
    AutoClosingCircularBuffer circularBuffer = new AutoClosingCircularBuffer(2);
    Closeable c1 = mock(Closeable.class);
    doThrow(new RuntimeException("Cannot free up this resource for some reason.")).when(c1).close();
    Closeable c2 = mock(Closeable.class);
    Closeable c3 = mock(Closeable.class);

    // When: middle element throws on close
    assertThrows(
        RuntimeException.class,
        () -> {
          circularBuffer.add(c1);
          circularBuffer.add(c2);
          circularBuffer.add(c3);
        });
  }
}
