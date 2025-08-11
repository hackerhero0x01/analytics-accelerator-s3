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

import java.util.Arrays;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import software.amazon.awssdk.services.s3.S3AsyncClient;

/** Kind of S3 Client used */
@AllArgsConstructor
@Getter
public enum S3ClientKind {
  SDK_V2_JAVA_ASYNC("ASYNC_JAVA"),
  SDK_V2_JAVA_SYNC("SYNC_JAVA"),
  SDK_V2_CRT_ASYNC("ASYNC_CRT"),
  FAULTY_S3_CLIENT("FAULTY_S3");

  private final String value;
  /**
   * Creates the S3 client based on the context and client kind. This is used by benchmarks, and
   * allows us easily run the same benchmarks against different clients and configurations
   *
   * @param s3ExecutionContext benchmark context
   * @return an instance of {@link S3AsyncClient}
   */
  public S3AsyncClient getS3Client(@NonNull S3ExecutionContext s3ExecutionContext) {
    switch (this) {
      case SDK_V2_JAVA_ASYNC:
        return s3ExecutionContext.getS3AsyncClient();
      case SDK_V2_CRT_ASYNC:
        return s3ExecutionContext.getS3CrtClient();
      case FAULTY_S3_CLIENT:
        return s3ExecutionContext.getFaultyS3Client();
      default:
        throw new IllegalArgumentException("Unsupported client kind: " + this);
    }
  }

  /**
   * Trusted S3 Clients
   *
   * @return small objects
   */
  public static List<S3ClientKind> trustedClients() {
    return Arrays.asList(SDK_V2_JAVA_ASYNC, SDK_V2_CRT_ASYNC, SDK_V2_JAVA_SYNC);
  }

  /**
   * Faulty S3 Clients
   *
   * @return small objects
   */
  public static List<S3ClientKind> faultyClients() {
    return Arrays.asList(FAULTY_S3_CLIENT);
  }
}
