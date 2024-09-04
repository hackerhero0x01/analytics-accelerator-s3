package com.amazon.connector.s3.util;

import com.amazon.connector.s3.S3SeekableInputStreamConfiguration;
import com.amazon.connector.s3.common.telemetry.Telemetry;
import com.amazon.connector.s3.io.logical.LogicalIO;
import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.logical.impl.DefaultLogicalIOImpl;
import com.amazon.connector.s3.io.logical.impl.ParquetLogicalIOImpl;
import com.amazon.connector.s3.io.logical.impl.ParquetMetadataStore;
import com.amazon.connector.s3.io.physical.data.BlobStore;
import com.amazon.connector.s3.io.physical.data.MetadataStore;
import com.amazon.connector.s3.io.physical.impl.PhysicalIOImpl;
import java.util.regex.Pattern;

/** A LogicalIO factory based on S3URI file extensions. */
public class LogicalIOFactory {

  /**
   * Constructs a logicalIO implementation based on the S3URI's file extension.
   *
   * @param s3URI the object this stream is using
   * @param metadataStore a MetadataStore instance being used as a HeadObject cache
   * @param blobStore a BlobStore instance being used as data cache
   * @param telemetry The {@link Telemetry} to use to report measurements.
   * @param configuration provides instance of {@link S3SeekableInputStreamConfiguration}
   * @param parquetMetadataStore object containing Parquet usage information
   * @return The logicalIO layer to use for reading.
   */
  // TODO: LogicalIO should not require ParquetMetadataStore at this point,
  //  https://github.com/awslabs/s3-connector-framework/issues/93
  public static LogicalIO getLogicalIOImplementation(
      S3URI s3URI,
      MetadataStore metadataStore,
      BlobStore blobStore,
      Telemetry telemetry,
      S3SeekableInputStreamConfiguration configuration,
      ParquetMetadataStore parquetMetadataStore) {

    if (isParquet(configuration.getLogicalIOConfiguration(), s3URI)) {
      return new ParquetLogicalIOImpl(
          s3URI,
          new PhysicalIOImpl(s3URI, metadataStore, blobStore, telemetry),
          telemetry,
          configuration.getLogicalIOConfiguration(),
          parquetMetadataStore);
    } else {
      return new DefaultLogicalIOImpl(
          new PhysicalIOImpl(s3URI, metadataStore, blobStore, telemetry));
    }
  }

  private static boolean isParquet(LogicalIOConfiguration configuration, S3URI s3URI) {
    Pattern parquetPattern =
        Pattern.compile(
            constructFileExtensionMatcher(configuration.getParquetFileSuffix()),
            Pattern.CASE_INSENSITIVE);

    return parquetPattern.matcher(s3URI.getKey()).find();
  }

  private static String constructFileExtensionMatcher(String fileExtension) {
    return new StringBuilder("^.*.(").append(fileExtension).append(")$").toString();
  }
}
