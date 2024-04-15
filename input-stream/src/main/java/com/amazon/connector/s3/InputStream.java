package com.amazon.connector.s3;

import java.util.Objects;

/**
 * High throughput stream used to read data from Amazon S3.
 */
public class InputStream {
    private final ObjectClient objectClient;

    /**
     * Creates a new instance of {@link InputStream}.
     *
     * @param objectClient an instance of {@link ObjectClient}.
     */
    public InputStream(ObjectClient objectClient) {
        Objects.requireNonNull(objectClient, "objectClient must not be null");
        this.objectClient = objectClient;
    }
}
