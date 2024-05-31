package com.amazon.connector.s3.io.logical;

import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.util.S3URI;
import lombok.Data;

import java.util.concurrent.CompletableFuture;

@Data
public class FileStatus {
    private final CompletableFuture<ObjectMetadata> objectMetadata;
    private final S3URI s3URI;
}
