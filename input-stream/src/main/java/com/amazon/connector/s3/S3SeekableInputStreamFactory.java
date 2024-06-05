package com.amazon.connector.s3;

import com.amazon.connector.s3.io.physical.blockmanager.BlockManager;
import com.amazon.connector.s3.io.physical.blockmanager.MultiObjectsBlockManager;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import lombok.Getter;
import lombok.NonNull;

/**
 * Initialises resources to prepare for reading from S3. Resources initialised in this class are
 * shared across instances of {@link S3SeekableInputStream}. For example, this allows for the same
 * S3 client to be used across multiple input streams for more efficient connection management etc.
 *
 * <p>This factory does NOT assume ownership of the passed {@link ObjectClient}. It is the
 * responsibility of the caller to close the client and to make sure that it remains active for
 * {@link S3SeekableInputStreamFactory#createStream(S3URI)} to vend correct {@link
 * SeekableInputStream}.
 */
@Getter
public class S3SeekableInputStreamFactory implements AutoCloseable {
  private final ObjectClient objectClient;
  private final S3SeekableInputStreamConfiguration configuration;
  private final MultiObjectsBlockManager multiObjectsBlockManager;

  /**
   * Creates a new instance of {@link S3SeekableInputStreamFactory}. This factory should be used to
   * create instances of the input stream to allow for sharing resources such as the object client
   * between streams.
   *
   * @param objectClient Object client
   * @param configuration {@link S3SeekableInputStream} configuration
   */
  public S3SeekableInputStreamFactory(
      @NonNull ObjectClient objectClient,
      @NonNull S3SeekableInputStreamConfiguration configuration) {
    this.objectClient = objectClient;
    this.configuration = configuration;
    this.multiObjectsBlockManager =
        new MultiObjectsBlockManager(objectClient, configuration.getBlockManagerConfiguration());
  }

  /**
   * Create an instance of S3SeekableInputStream.
   *
   * @param s3URI the object's S3 URI
   * @return An instance of the input stream.
   */
  public S3SeekableInputStream createStream(@NonNull S3URI s3URI) {
    if (configuration.getBlockManagerConfiguration().isUseSingleCache()) {
      BlockManager blockManager = new BlockManager(multiObjectsBlockManager, s3URI);
      return new S3SeekableInputStream(blockManager, configuration);
    }

    return new S3SeekableInputStream(objectClient, s3URI, configuration);
  }

  /**
   * Closes the factory and underlying resources.
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    multiObjectsBlockManager.close();
  }
}
