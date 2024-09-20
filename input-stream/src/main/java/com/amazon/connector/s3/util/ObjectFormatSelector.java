package com.amazon.connector.s3.util;

import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import java.util.regex.Pattern;

/** A LogicalIO factory based on S3URI file extensions. */
public class ObjectFormatSelector {

  private final Pattern parquetPattern;

  /**
   * Creates a new instance of {@ObjectFormatSelector}. Used to select the file format of a
   * particular object key.
   *
   * @param configuration LogicalIO configuration.
   */
  public ObjectFormatSelector(LogicalIOConfiguration configuration) {
    this.parquetPattern =
        Pattern.compile(configuration.getParquetFormatSelectorRegex(), Pattern.CASE_INSENSITIVE);
  }

  /**
   * Uses a regex matcher to select the file format based on the file extension of the key.
   *
   * @param s3URI the object's S3 URI
   * @return the file format of the object
   */
  public ObjectFormat getObjectFormat(S3URI s3URI) {
    if (parquetPattern.matcher(s3URI.getKey()).find()) {
      return ObjectFormat.PARQUET;
    }

    return ObjectFormat.DEFAULT;
  }
}
