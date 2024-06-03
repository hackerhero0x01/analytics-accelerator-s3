package com.amazon.connector.s3.property;

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
  boolean positionIsInitiallyZero(@ForAll("streamSizes") int size) {
    InMemoryS3SeekableStream s = new InMemoryS3SeekableStream("test-bucket", "test-key", size);

    return s.getPos() == 0;
  }

  @Property
  boolean seekChangesPosition(
      @ForAll("positiveStreamSizes") int size, @ForAll("validPositions") int pos)
      throws IOException {
    InMemoryS3SeekableStream s = new InMemoryS3SeekableStream("test-bucket", "test-key", size);

    int jumpInSideObject = pos % size;
    s.seek(jumpInSideObject);
    return s.getPos() == jumpInSideObject;
  }

  @Property
  void seekToInvalidPositionThrows(@ForAll("invalidPositions") int invalidPos) {
    InMemoryS3SeekableStream s = new InMemoryS3SeekableStream("test-bucket", "test-key", 42);

    assertThrows(IllegalArgumentException.class, () -> s.seek(invalidPos));
  }
}
