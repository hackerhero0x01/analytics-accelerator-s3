package com.amazon.connector.s3.io.logical.parquet;

import java.util.HashMap;
import java.util.List;
import lombok.Getter;

/** Mappings of parquet column file offset index to column name and vice versa. */
public class ColumnMappers {
  @Getter private final HashMap<String, ColumnMetadata> offsetIndexToColumnMap = new HashMap<>();

  @Getter
  private final HashMap<String, List<ColumnMetadata>> columnNameToOffsetIndexMap = new HashMap<>();
}
