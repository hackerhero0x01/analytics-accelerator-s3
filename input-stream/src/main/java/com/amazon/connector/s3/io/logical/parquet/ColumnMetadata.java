package com.amazon.connector.s3.io.logical.parquet;

import lombok.Data;

/** Container for storing necessary parquet column information. */
@Data
public class ColumnMetadata {
  private final int rowGroupIndex;
  private final String columnName;
  private final long startPos;
  private final long compressedSize;
  private final int schemaHash;
}
