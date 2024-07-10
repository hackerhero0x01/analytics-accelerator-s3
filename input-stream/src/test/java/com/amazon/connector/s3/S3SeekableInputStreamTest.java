package com.amazon.connector.s3;

import static com.amazon.connector.s3.util.Constants.ONE_MB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

import com.amazon.connector.s3.io.logical.LogicalIO;
import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.logical.impl.ParquetLogicalIOImpl;
import com.amazon.connector.s3.io.logical.impl.ParquetMetadataStore;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.io.physical.blockmanager.BlockManager;
import com.amazon.connector.s3.io.physical.blockmanager.BlockManagerConfiguration;
import com.amazon.connector.s3.io.physical.blockmanager.BlockManagerInterface;
import com.amazon.connector.s3.io.physical.blockmanager.MultiObjectsBlockManager;
import com.amazon.connector.s3.io.physical.impl.PhysicalIOImpl;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.util.FakeObjectClient;
import com.amazon.connector.s3.util.S3URI;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.utils.IoUtils;
import software.amazon.awssdk.utils.StringUtils;

public class S3SeekableInputStreamTest extends S3SeekableInputStreamTestBase {

  private final ExecutorService asyncProcessingPool =
      Executors.newFixedThreadPool(LogicalIOConfiguration.DEFAULT.getParquetParsingPoolSize());

  @Test
  void testConstructor() {
    S3SeekableInputStream inputStream = new S3SeekableInputStream(TEST_OBJECT, fakeLogicalIO);
    assertNotNull(inputStream);
  }

  @Test
  void testDefaultConstructor() throws IOException {
    S3URI s3URI = S3URI.of("bucket", "key");

    S3SeekableInputStream inputStream =
        new S3SeekableInputStream(
            fakeObjectClient,
            s3URI,
            S3SeekableInputStreamConfiguration.DEFAULT,
            new ParquetMetadataStore(LogicalIOConfiguration.DEFAULT),
            asyncProcessingPool);
    assertNotNull(inputStream);

    BlockManager blockManager =
        new BlockManager(fakeObjectClient, s3URI, BlockManagerConfiguration.DEFAULT);
    inputStream =
        new S3SeekableInputStream(
            TEST_OBJECT,
            blockManager,
            S3SeekableInputStreamConfiguration.DEFAULT,
            new ParquetMetadataStore(LogicalIOConfiguration.DEFAULT),
            asyncProcessingPool);
    assertNotNull(inputStream);
  }

  @Test
  void testConstructorThrowsOnNullArgument() {
    S3SeekableInputStreamConfiguration conf = S3SeekableInputStreamConfiguration.DEFAULT;
    S3URI s3URI = S3URI.of("bucket", "key");
    assertThrows(
        NullPointerException.class,
        () ->
            new S3SeekableInputStream(
                fakeObjectClient,
                (S3URI) null,
                conf,
                new ParquetMetadataStore(LogicalIOConfiguration.DEFAULT),
                asyncProcessingPool));

    assertThrows(
        NullPointerException.class,
        () ->
            new S3SeekableInputStream(
                null,
                s3URI,
                conf,
                new ParquetMetadataStore(LogicalIOConfiguration.DEFAULT),
                asyncProcessingPool));

    assertThrows(
        NullPointerException.class,
        () ->
            new S3SeekableInputStream(
                fakeObjectClient,
                s3URI,
                null,
                new ParquetMetadataStore(LogicalIOConfiguration.DEFAULT),
                asyncProcessingPool));

    assertThrows(
        NullPointerException.class,
        () ->
            new S3SeekableInputStream(
                null,
                (S3URI) null,
                conf,
                new ParquetMetadataStore(LogicalIOConfiguration.DEFAULT),
                asyncProcessingPool));

    assertThrows(
        NullPointerException.class,
        () ->
            new S3SeekableInputStream(
                null,
                s3URI,
                null,
                new ParquetMetadataStore(LogicalIOConfiguration.DEFAULT),
                asyncProcessingPool));

    assertThrows(
        NullPointerException.class,
        () ->
            new S3SeekableInputStream(
                fakeObjectClient,
                null,
                null,
                new ParquetMetadataStore(LogicalIOConfiguration.DEFAULT),
                asyncProcessingPool));

    assertThrows(
        NullPointerException.class,
        () ->
            new S3SeekableInputStream(
                TEST_OBJECT,
                (BlockManagerInterface) null,
                conf,
                new ParquetMetadataStore(LogicalIOConfiguration.DEFAULT),
                asyncProcessingPool));
    BlockManager blockManager =
        new BlockManager(fakeObjectClient, s3URI, BlockManagerConfiguration.DEFAULT);
    assertThrows(
        NullPointerException.class,
        () ->
            new S3SeekableInputStream(
                null,
                blockManager,
                null,
                new ParquetMetadataStore(LogicalIOConfiguration.DEFAULT),
                asyncProcessingPool));
    assertThrows(NullPointerException.class, () -> new S3SeekableInputStream(null, null));

    assertThrows(
        NullPointerException.class, () -> new S3SeekableInputStream(TEST_OBJECT, (LogicalIO) null));
  }

  @Test
  void testInitialGetPosition() throws IOException {
    // Given
    S3SeekableInputStream stream = new S3SeekableInputStream(TEST_OBJECT, fakeLogicalIO);

    // When: nothing
    // Then: stream position is at 0
    assertEquals(0, stream.getPos());
  }

  @Test
  void testReadAdvancesPosition() throws IOException {
    // Given
    S3SeekableInputStream stream = new S3SeekableInputStream(TEST_OBJECT, fakeLogicalIO);

    // When: read() is called
    stream.read();

    // Then: position is advanced
    assertEquals(1, stream.getPos());
  }

  @Test
  void testSeek() throws IOException {
    // Given
    S3SeekableInputStream stream = new S3SeekableInputStream(TEST_OBJECT, fakeLogicalIO);

    // When
    stream.seek(13);

    // Then
    assertEquals(13, stream.getPos());
  }

  @Test
  void testFullRead() throws IOException {
    // Given
    S3SeekableInputStream stream = new S3SeekableInputStream(TEST_OBJECT, fakeLogicalIO);

    // When: all data is requested
    String dataReadOut = IoUtils.toUtf8String(stream);

    // Then: data read out is the same as data under stream
    assertEquals(TEST_DATA, dataReadOut);
  }

  @Test
  void testSeekToVeryEnd() throws IOException {
    // Given
    S3SeekableInputStream stream = new S3SeekableInputStream(TEST_OBJECT, fakeLogicalIO);

    // When: we seek to the last byte
    stream.seek(TEST_DATA.length() - 1);

    // Then: first read returns the last byte and the next read returns -1
    assertEquals(48, stream.read());
    assertEquals(-1, stream.read());
  }

  @Test
  void testSeekAfterEnd() throws IOException {
    // Given
    S3SeekableInputStream stream = new S3SeekableInputStream(TEST_OBJECT, fakeLogicalIO);

    // When: we seek past EOF we get EOFException
    assertThrows(EOFException.class, () -> stream.seek(TEST_DATA.length() + 1));
  }

  @Test
  void testReadOnEmptyObject() throws IOException {
    // Given
    S3SeekableInputStream stream = getTestStreamWithContent("");

    // When: we read a byte from the empty object
    int readByte = stream.read();

    // Then: read returns -1
    assertEquals(-1, readByte);
  }

  @Test
  void testInvalidSeek() throws IOException {
    // Given
    S3SeekableInputStream stream = getTestStream();

    // When: seek is to an invalid position then exception is thrown
    assertThrows(Exception.class, () -> stream.seek(TEST_DATA.length()));
    assertThrows(Exception.class, () -> stream.seek(TEST_DATA.length() + 10));
    assertThrows(Exception.class, () -> stream.seek(Long.MAX_VALUE));
    assertThrows(Exception.class, () -> stream.seek(-1));
    assertThrows(Exception.class, () -> stream.seek(Long.MIN_VALUE));
  }

  @Test
  void testLogicalIOGetsClosed() throws IOException {
    // Given
    LogicalIO logicalIO = mock(LogicalIO.class);
    S3SeekableInputStream stream = new S3SeekableInputStream(TEST_OBJECT, logicalIO);

    // When
    stream.close();

    // Then
    verify(logicalIO, times(1)).close();
  }

  @Test
  void testReadWithBuffer() throws IOException {
    S3SeekableInputStream stream = getTestStream();

    byte[] buffer = new byte[TEST_DATA.length()];
    assertEquals(20, stream.read(buffer, 0, TEST_DATA.length()));
    assertTrue(Arrays.equals(buffer, TEST_DATA.getBytes()));
    assertEquals(stream.getPos(), TEST_DATA.length());

    // All data has been read, and pos is current at EOF. Next read should return -1.
    assertEquals(-1, stream.read(buffer, 0, TEST_DATA.length()));
  }

  @Test
  void testReadWithBufferAndSeeks() throws IOException {
    S3SeekableInputStream stream = getTestStream();

    byte[] buffer = new byte[11];

    // Read from pos 0, check pos after read is correct
    stream.read(new byte[3], 0, 3);
    assertEquals(stream.getPos(), 3);

    byte[] expectedResult =
        ByteBuffer.wrap(new byte[11])
            .put(new byte[3])
            .put(TEST_DATA.substring(3, 11).getBytes())
            .array();

    assertEquals(8, stream.read(buffer, 3, 8));
    assertTrue(Arrays.equals(buffer, expectedResult));
    assertEquals(stream.getPos(), 11);
    assertEquals(stream.read(), TEST_DATA.getBytes()[11]);

    // Check things still work after a backward seek
    stream.seek(4);
    byte[] readBuffer = new byte[7];
    assertEquals(7, stream.read(readBuffer, 0, 7));
    assertEquals(11, stream.getPos());
    assertTrue(Arrays.equals(readBuffer, TEST_DATA.substring(4, 11).getBytes()));
  }

  @Test
  void testReadWithBufferOutOfBounds() throws IOException {
    S3SeekableInputStream stream = getTestStream();

    // Read beyond EOF, expect all bytes to be read and pos to be EOF.
    assertEquals(TEST_DATA.length(), stream.read(new byte[20], 0, TEST_DATA.length() + 20));
    assertEquals(20, stream.getPos());

    // Read beyond EOF after a seek, expect only num bytes read to be equal to that left in the
    // stream, and pos to be EOF.
    stream.seek(18);
    assertEquals(2, stream.read(new byte[20], 0, TEST_DATA.length() + 20));
    assertEquals(20, stream.getPos());
  }

  @Test
  void testReadTailWithInvalidArgument() {
    // Given: seekable stream
    S3SeekableInputStream stream = getTestStream();

    // When & Then: reading tail with invalid arguments, exception is thrown
    // -1 is invalid length
    assertThrows(IllegalArgumentException.class, () -> stream.readTail(new byte[3], 0, -1));
    // 100K is bigger than test data size
    assertThrows(IllegalArgumentException.class, () -> stream.readTail(new byte[103], 0, 100));
    // Requesting more data than byte buffer size
    assertThrows(IllegalArgumentException.class, () -> stream.readTail(new byte[10], 0, 100));
  }

  @Test
  void testReadTailHappyCase() throws IOException {
    // Given: seekable stream
    S3SeekableInputStream stream = getTestStream();

    // When: tail of length 10 is requested
    byte[] buf = new byte[11];
    int numBytesRead = stream.readTail(buf, 0, buf.length);

    // Then: 10 bytes are read, 10 is returned, 10 bytes in the buffer are the same as last 10 bytes
    // of test data
    assertEquals(11, numBytesRead);
    assertEquals("12345678910", new String(buf, StandardCharsets.UTF_8));
  }

  @Test
  void testReadTailDoesNotAlterPosition() throws IOException {
    // Given: seekable stream
    S3SeekableInputStream stream = getTestStream();

    // When: 1) we are reading from the stream, 2) reading the tail of the stream, 3) reading more
    // from the stream
    byte[] one = new byte[5];
    byte[] two = new byte[11];
    byte[] three = new byte[5];

    int numBytesRead1 = stream.read(one, 0, one.length);
    int numBytesRead2 = stream.readTail(two, 0, two.length);
    int numBytesRead3 = stream.read(three, 0, three.length);

    // Then: read #2 did not alter the position and reads #1 and #3 return subsequent bytes
    assertEquals(5, numBytesRead1);
    assertEquals(11, numBytesRead2);
    assertEquals(5, numBytesRead3);

    assertEquals("test-", new String(one, StandardCharsets.UTF_8));
    assertEquals("data1", new String(three, StandardCharsets.UTF_8));
    assertEquals("12345678910", new String(two, StandardCharsets.UTF_8));
  }

  @Test
  // Dependencies returning with a -1 read should not set the position back
  void testMinusOneIsHandledProperly() throws IOException {
    // Given: seekable stream
    LogicalIO mockLogicalIO = mock(LogicalIO.class);
    when(mockLogicalIO.metadata())
        .thenReturn(
            CompletableFuture.completedFuture(ObjectMetadata.builder().contentLength(200).build()));
    S3SeekableInputStream stream = new S3SeekableInputStream(TEST_OBJECT, mockLogicalIO);

    // When: logical IO returns with a -1 read
    final int INITIAL_POS = 123;
    stream.seek(INITIAL_POS);
    when(mockLogicalIO.read(any(), anyInt(), anyInt(), anyLong())).thenReturn(-1);

    // Then: stream returns -1 and position did not change
    final int LEN = 5;
    byte[] b = new byte[LEN];
    assertEquals(-1, stream.read(b, 0, LEN));
    assertEquals(INITIAL_POS, stream.getPos());
  }

  @Test
  void testMultiThreadUsage() throws IOException, InterruptedException {
    int filesSize = 8 * ONE_MB;
    StringBuilder sb = new StringBuilder(filesSize);
    sb.append(StringUtils.repeat("0", 8 * ONE_MB));
    S3URI s3URI = S3URI.of("test", "test");
    MultiObjectsBlockManager blockManager =
        new MultiObjectsBlockManager(
            new FakeObjectClient(sb.toString()), BlockManagerConfiguration.DEFAULT);
    AtomicBoolean haveException = new AtomicBoolean(false);

    // Create 20 threads to start multiple SeekableInputStream to read last and first 4 bytes
    ArrayList<Thread> threads = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      threads.add(
          new Thread(
              () -> {
                try {
                  PhysicalIO physicalIO = new PhysicalIOImpl(new BlockManager(blockManager, s3URI));
                  LogicalIO logicalIO =
                      new ParquetLogicalIOImpl(
                          TEST_OBJECT,
                          physicalIO,
                          LogicalIOConfiguration.DEFAULT,
                          new ParquetMetadataStore(LogicalIOConfiguration.DEFAULT),
                          asyncProcessingPool);
                  SeekableInputStream stream = new S3SeekableInputStream(TEST_OBJECT, logicalIO);
                  byte[] buffer = new byte[4];
                  stream.readTail(buffer, 0, 4);
                  stream.read(buffer, 0, 4);
                } catch (Exception e) {
                  haveException.set(true);
                  Thread.currentThread().interrupt();
                }
              }));
    }
    // Start all the threads
    threads.forEach(Thread::start);

    for (Thread thread : threads) {
      try {
        thread.join();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    boolean completedWithException = haveException.get();
    assertFalse(completedWithException, "Have exception in one of the threads");
  }

  private S3SeekableInputStream getTestStream() {
    return new S3SeekableInputStream(TEST_OBJECT, fakeLogicalIO);
  }

  private S3SeekableInputStream getTestStreamWithContent(String content) {
    LogicalIOConfiguration configuration =
        LogicalIOConfiguration.builder().footerCachingEnabled(false).build();

    return new S3SeekableInputStream(
        TEST_OBJECT,
        new ParquetLogicalIOImpl(
            TEST_OBJECT,
            new PhysicalIOImpl(
                new BlockManager(
                    new FakeObjectClient(content), TEST_OBJECT, BlockManagerConfiguration.DEFAULT)),
            configuration,
            new ParquetMetadataStore(configuration),
            asyncProcessingPool));
  }
}
