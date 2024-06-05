package com.amazon.connector.s3.io.physical.plan;

import lombok.Builder;
import lombok.Getter;

import java.util.List;

/** A logical IO plan */
@Builder
@Getter
public class IOPlan {
    List<Range> prefetchRanges;
}
