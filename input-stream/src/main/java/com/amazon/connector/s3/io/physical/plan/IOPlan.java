package com.amazon.connector.s3.io.physical.plan;

import java.util.List;
import lombok.Builder;
import lombok.Getter;

/** A logical IO plan */
@Builder
@Getter
public class IOPlan {
  List<Range> prefetchRanges;
}
