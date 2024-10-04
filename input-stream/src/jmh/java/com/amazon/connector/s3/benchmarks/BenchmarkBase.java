package com.amazon.connector.s3.benchmarks;

import static com.amazon.connector.s3.benchmarks.data.generation.Constants.ONE_MB_IN_BYTES;

import com.amazon.connector.s3.benchmarks.common.*;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;
import lombok.NonNull;
import org.openjdk.jmh.annotations.*;
import software.amazon.awssdk.services.s3.S3AsyncClient;

/**
 * Base class for benchmarks that iterate through the client types and stream types All derived
 * benchmarks are going to be run with Java Async and CRT clients, as well as SDK GET streams as the
 * seekable streams
 */
@State(Scope.Benchmark)
public abstract class BenchmarkBase {
  @Getter @Param S3ClientKind client;
  @Getter @Param S3InputStreamKind stream;

  @NonNull private final AtomicReference<BenchmarkContext> benchmarkContext = new AtomicReference<>();

  /**
   * Sets up the benchmarking context
   *
   * @throws IOException thrown on IO error
   */
  @Setup(Level.Trial)
  public void setUp() throws IOException {
    this.benchmarkContext.getAndSet(new BenchmarkContext(BenchmarkConfiguration.fromEnvironment()));
  }

  /**
   * Tears down benchmarking context
   *
   * @throws IOException thrown on IO error
   */
  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    this.benchmarkContext.getAndSet(null).close();
  }

  /**
   * Returns benchmark context
   *
   * @return benchmark context
   */
  public BenchmarkContext getBenchmarkContext() {
    return this.benchmarkContext.get();
  }

  /**
   * Runs the benchmarks
   *
   * @param benchmarkObject {@link BenchmarkObject}
   */
  protected final void run(BenchmarkObject benchmarkObject)
      throws IOException, InterruptedException {
    System.out.println(
        String.format(
            "{stream: \"%s\", client: \"%s\", object: \"%s\"}",
            this.getStream(), this.getClient(), benchmarkObject.toString()));

    switch (this.getStream()) {
      case S3_SDK_GET:
        processS3RawStream(benchmarkObject);
        break;
      case S3_DAT_GET:
        processS3DATStream(benchmarkObject);
        break;
      default:
        throw new IllegalArgumentException("Unknown S3InputStreamKind: " + this.getStream());
    }
  }

  /**
   * Processes with {@link S3AsyncClient}
   *
   * @param benchmarkObject {@link BenchmarkObject}
   * @throws IOException IO error, if encountered
   */
  protected abstract void processS3RawStream(BenchmarkObject benchmarkObject)
      throws IOException, InterruptedException;

  /**
   * Processes with {@link com.amazon.connector.s3.S3SeekableInputStream}
   *
   * @param benchmarkObject {@link BenchmarkObject}
   * @throws IOException IO error, if encountered
   */
  protected abstract void processS3DATStream(BenchmarkObject benchmarkObject)
      throws IOException, InterruptedException;

  /**
   * Consumes the stream by reading
   *
   * @param inputStream the {@link InputStream}
   * @param benchmarkObject {@link BenchmarkObject}
   * @param length of data to read
   * @throws IOException thrown on IO error
   */
  protected void drainStream(
      @NonNull InputStream inputStream, @NonNull BenchmarkObject benchmarkObject, long length)
      throws IOException {
    // Set buffer size to min between the object size and whatever the config specifies
    int bufferSize =
        (int)
            Math.min(
                (long) this.getBenchmarkContext().getConfiguration().getBufferSizeMb()
                    * ONE_MB_IN_BYTES,
                benchmarkObject.getSize());
    byte[] b = new byte[bufferSize];
    // Adjust the size to not exceed the object size
    length = Math.min(length, benchmarkObject.getSize());

    // Now just read sequentially
    long totalReadBytes = 0;
    int readBytes = 0;
    do {
      // determine the size to read - it's either the buffer size or less, if we are getting to the
      // end
      int readSize = (int) Math.min(bufferSize, length - readBytes);
      if (readSize <= 0) {
        break;
      }
      readBytes = inputStream.read(b, 0, readSize);
      if (readBytes > 0) {
        totalReadBytes += readBytes;
      }
    } while (readBytes > 0);

    // Verify that we have drained the whole thing
    if (totalReadBytes != length) {
      throw new IllegalStateException(
          "Read " + totalReadBytes + " bytes but expected " + benchmarkObject.getSize());
    }
  }
}
