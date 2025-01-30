/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package software.amazon.s3.analyticsaccelerator.access;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.DelegatingS3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/**
 * A faulty S3 Client implementation that injects failure to S3 calls. Current version only adds
 * infinite waiting to Get calls. We should extend this in the future to except a FaultKind or a
 * FaultConfiguration.
 */
public class FaultyS3AsyncClient extends DelegatingS3AsyncClient {

  Set<String> stuckObjects;

  /**
   * Create an instance of a S3 client, for interaction with Amazon S3 compatible object stores.
   * This takes ownership of the passed client and will close it on its own close().
   *
   * @param delegate Underlying client to be used for making requests to S3.
   */
  public FaultyS3AsyncClient(S3AsyncClient delegate) {
    super(delegate);
    this.stuckObjects = Collections.synchronizedSet(new HashSet<>());
  }

  /**
   * A faulty override of getObject. First GET call to an object will wait forever. All requests to
   * the same object will be delegated to regular S3AsyncClient.
   *
   * @param request request
   * @param asyncResponseTransformer response transformer
   * @return an instance of {@link S3AsyncClient}
   */
  @Override
  public <T> CompletableFuture<T> getObject(
      GetObjectRequest request,
      AsyncResponseTransformer<GetObjectResponse, T> asyncResponseTransformer) {
    if (!stuckObjects.contains(request.key())) {
      stuckObjects.add(request.key());
      return CompletableFuture.supplyAsync(
          () -> {
            try {
              while (true) {
                Thread.sleep(10000);
              }
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
            return null;
          });
    } else {
      return super.getObject(request, asyncResponseTransformer);
    }
  }
}
