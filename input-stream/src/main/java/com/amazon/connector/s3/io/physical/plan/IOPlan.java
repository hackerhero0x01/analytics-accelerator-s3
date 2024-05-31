package com.amazon.connector.s3.io.physical.plan;

import com.amazon.connector.s3.io.logical.FileStatus;
import lombok.Builder;
import lombok.Getter;

import java.util.List;

/** A logical IO plan */
@Builder
@Getter
public class IOPlan {
    List<Range> prefetchRanges;
    FileStatus fileStatus;
}
