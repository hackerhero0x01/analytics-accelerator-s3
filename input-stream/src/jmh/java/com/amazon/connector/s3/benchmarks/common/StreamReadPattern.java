package com.amazon.connector.s3.benchmarks.common;

import java.util.List;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

/** Represents a read pattern - a sequence of reads */
@Value
@Builder
public class StreamReadPattern {
  @Singular List<StreamRead> streamReads;
}
