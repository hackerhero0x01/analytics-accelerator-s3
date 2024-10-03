package com.amazon.connector.s3.property;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.amazon.connector.s3.arbitraries.StreamArbitraries;
import java.io.IOException;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;

/**
 * A set of high level properties that need to pass for ANY correct seekable stream implementation
 */
public class SeekableStreamPropertiesTest extends StreamArbitraries {

  @Property
  boolean positionIsInitiallyZero(@ForAll("streamSizes") int size) throws IOException {
    try (InMemoryS3SeekableInputStream s =
        new InMemoryS3SeekableInputStream("test-bucket", "test-key", size)) {

      return s.getPos() == 0;
    }
  }

  @Property
  boolean seekChangesPosition(
      @ForAll("positiveStreamSizes") int size, @ForAll("validPositions") int pos)
      throws IOException {
    try (InMemoryS3SeekableInputStream s =
        new InMemoryS3SeekableInputStream("test-bucket", "test-key", size)) {

      int jumpInSideObject = pos % size;
      s.seek(jumpInSideObject);
      return s.getPos() == jumpInSideObject;
    }
  }

  @Property
  void readIncreasesPosition(
      @ForAll("sizeBiggerThanOne") int size, @ForAll("validPositions") int pos) throws IOException {
    try (InMemoryS3SeekableInputStream s =
        new InMemoryS3SeekableInputStream("test-bucket", "test-key", size)) {

      int newPos = Math.max(0, pos % size - 1);

      // Seek is correct
      s.seek(newPos);
      assertEquals(newPos, s.getPos());

      // Read increases position by 1
      s.read();
      assertEquals(newPos + 1, s.getPos());
    }
  }

  @Property
  void seekToInvalidPositionThrows(@ForAll("invalidPositions") int invalidPos) throws IOException {
    try (InMemoryS3SeekableInputStream s =
        new InMemoryS3SeekableInputStream("test-bucket", "test-key", 42)) {

      assertThrows(IllegalArgumentException.class, () -> s.seek(invalidPos));
    }
  }

  @Property
  void canCloseStreamMultipleTimes(@ForAll("streamSizes") int size) throws IOException {
    InMemoryS3SeekableInputStream s =
        new InMemoryS3SeekableInputStream("test-bucket", "test-key", size);
    s.close();
    s.close();
  }
}
