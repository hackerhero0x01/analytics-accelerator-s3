package com.amazon.connector.s3;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class S3SdkObjectClientTest {
    @Test
    void testConstructor() {
        S3SdkObjectClient client = new S3SdkObjectClient();
        assertNotNull(client);
    }
}
