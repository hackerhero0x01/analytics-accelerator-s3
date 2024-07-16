package com.amazon.connector.s3.benchmark;

import com.amazon.connector.s3.S3SdkObjectClient;
import com.amazon.connector.s3.S3SeekableInputStream;
import com.amazon.connector.s3.S3SeekableInputStreamConfiguration;
import com.amazon.connector.s3.S3SeekableInputStreamFactory;
import com.amazon.connector.s3.datagen.Constants;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import software.amazon.awssdk.services.s3.S3AsyncClient;

/** Benchmark simulating an EQ2 query. */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 2)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.SingleShotTime)
public class EQ2Simulator {

  private static final String KEY = "part-0-c1c1ff96-e2fa-4ff6-81f2-80ac39bdafbf.parquet";

  /** Test sequential reads with seekable streams. */
  @Benchmark
  public void testSequentialRead__withSeekableStream__eq2Parquet() throws IOException {

    S3SeekableInputStreamFactory s3SeekableInputStreamFactory =
        new S3SeekableInputStreamFactory(
            new S3SdkObjectClient(S3AsyncClient.crtBuilder().maxConcurrency(300).build()),
            S3SeekableInputStreamConfiguration.DEFAULT);

    S3SeekableInputStream stream =
        s3SeekableInputStreamFactory.createStream(
            S3URI.of(Constants.BENCHMARK_BUCKET, Constants.BENCHMARK_DATA_PREFIX_SEQUENTIAL + KEY));

    byte[] b_1 = new byte[1];
    byte[] b_4 = new byte[4];
    byte[] b_6015 = new byte[6_015];
    byte[] b_690412 = new byte[690_412];
    byte[] b_8388608 = new byte[8_388_608];

    // START
    stream.seek(107190159);
    stream.read();
    stream.read();
    stream.read();
    stream.read();
    stream.read(b_4, 0, 4);

    stream.seek(107184144);
    stream.read(b_6015, 0, 6015);

    stream.seek(10091506);
    for (int i = 0; i < 25; ++i) {
      stream.read(b_1, 0, 1);
    }
    stream.read(b_690412, 0, 690412);
    stream.seek(4);
    for (int i = 0; i < 13; ++i) {
      stream.read(b_8388608, 0, b_8388608.length);
    }

    // END
    stream.close();
    s3SeekableInputStreamFactory.close();
  }
}
