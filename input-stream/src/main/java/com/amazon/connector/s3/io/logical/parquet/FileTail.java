package com.amazon.connector.s3.io.logical.parquet;

import java.nio.ByteBuffer;
import lombok.Data;

/** Container for tail of a parquet file. */
@Data
public class FileTail {
  private final ByteBuffer fileTail;
  private final int fileTailLength;
}
