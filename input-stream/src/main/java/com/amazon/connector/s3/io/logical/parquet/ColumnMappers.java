package com.amazon.connector.s3.io.logical.parquet;

import java.util.HashMap;
import lombok.Data;

/** Mappings of parquet column file offset index to column name and vice versa. */
@Data
public class ColumnMappers {
  private final HashMap<String, ColumnMetadata> offsetIndexToColumnMap;
  private final HashMap<String, ColumnMetadata> columnNameToColumnMap;
}
