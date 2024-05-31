package com.amazon.connector.s3.io.physical.plan;

import lombok.Data;

@Data
public class Range {
    private final long start;
    private final long end;
}
