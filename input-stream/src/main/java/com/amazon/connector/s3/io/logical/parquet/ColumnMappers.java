package com.amazon.connector.s3.io.logical.parquet;

import java.util.HashMap;
import java.util.List;
import lombok.Value;

/** Mappings of parquet column file offset index to column name and vice versa. */
@Value
public class ColumnMappers {
  HashMap<Long, ColumnMetadata> offsetIndexToColumnMap;
  HashMap<String, List<ColumnMetadata>> columnNameToColumnMap;
}
