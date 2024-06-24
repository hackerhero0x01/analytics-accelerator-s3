package com.amazon.connector.s3.io.logical.parquet;

import java.util.HashMap;
import java.util.List;
import lombok.Data;

/** Mappings of parquet column file offset index to column name and vice versa. */
@Data
public class ColumnMappers {
  private final HashMap<Long, ColumnMetadata> offsetIndexToColumnMap;
  private final HashMap<String, List<ColumnMetadata>> columnNameToColumnMap;
}
