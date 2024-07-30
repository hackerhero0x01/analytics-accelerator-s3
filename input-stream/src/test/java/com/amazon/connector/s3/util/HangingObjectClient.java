package com.amazon.connector.s3.util;

import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.object.ObjectContent;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.request.GetRequest;
import com.amazon.connector.s3.request.HeadRequest;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.io.input.NullInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HangingObjectClient implements ObjectClient {

  private static final Logger LOG = LoggerFactory.getLogger(HangingObjectClient.class);

  @Override
  public CompletableFuture<ObjectMetadata> headObject(HeadRequest headRequest) {
    return CompletableFuture.supplyAsync(
        () -> {
          while (true) {
            try {
              Thread.sleep(100_000);
            } catch (InterruptedException e) {
              LOG.error("Thread got interrupted in HangingObjectClient", e);
              return ObjectMetadata.builder().contentLength(100).build();
            }
          }
        });
  }

  @Override
  public CompletableFuture<ObjectContent> getObject(GetRequest getRequest) {
    return CompletableFuture.supplyAsync(
        () -> {
          while (true) {
            try {
              Thread.sleep(100_000);
            } catch (InterruptedException e) {
              LOG.error("Thread got interrupted in HangingObjectClient", e);
              return ObjectContent.builder().stream(new NullInputStream()).build();
            }
          }
        });
  }

  @Override
  public void close() throws IOException {}
}
