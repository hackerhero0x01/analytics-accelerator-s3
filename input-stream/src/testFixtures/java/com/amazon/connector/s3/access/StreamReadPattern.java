package com.amazon.connector.s3.access;

import java.util.List;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;

/** Represents a read pattern - a sequence of reads */
@Value
@Builder
public class StreamReadPattern {
  @Singular @NonNull List<StreamRead> streamReads;
}
