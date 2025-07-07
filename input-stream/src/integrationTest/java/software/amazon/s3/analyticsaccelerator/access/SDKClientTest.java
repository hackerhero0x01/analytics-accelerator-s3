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

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.s3.analyticsaccelerator.ObjectClientConfiguration;
import software.amazon.s3.analyticsaccelerator.S3SdkObjectClient;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

public class SDKClientTest extends IntegrationTestBase {

  @ParameterizedTest
  @MethodSource("clientKinds")
  void testSettingUserAgent(S3ClientKind clientKind) throws IOException {
    ObjectClientConfiguration objectClientConfiguration =
        ObjectClientConfiguration.builder().userAgentPrefix("DummyUserAgent").build();
    S3SdkObjectClient client =
        new S3SdkObjectClient(
            clientKind.getS3Client(getS3ExecutionContext()), objectClientConfiguration);
    assertNotNull(client);
    assertDoesNotThrow(() -> readWithCustomClient(client));
  }

  @ParameterizedTest
  @MethodSource("clientKinds")
  void testNullUserAgent(S3ClientKind clientKind) {
    ObjectClientConfiguration objectClientConfiguration =
        ObjectClientConfiguration.builder().build();
    S3SdkObjectClient client =
        new S3SdkObjectClient(
            clientKind.getS3Client(getS3ExecutionContext()), objectClientConfiguration);
    assertDoesNotThrow(() -> readWithCustomClient(client));
  }

  @ParameterizedTest
  @MethodSource("clientKinds")
  void testExistingClientWithUserAgent(S3ClientKind clientKind) {
    ObjectClientConfiguration objectClientConfiguration =
        ObjectClientConfiguration.builder().build();
    S3SdkObjectClient client =
        new S3SdkObjectClient(
            clientKind.getS3Client(getS3ExecutionContext()), objectClientConfiguration);
    assertNotNull(client);
    assertDoesNotThrow(() -> readWithCustomClient(client));
  }

  private void readWithCustomClient(S3SdkObjectClient client) throws IOException {
    S3Object object = S3Object.RANDOM_16MB;
    S3URI s3URI = object.getObjectUri(this.getS3ExecutionContext().getConfiguration().getBaseUri());
    S3SeekableInputStreamFactory factory =
        new S3SeekableInputStreamFactory(client, S3SeekableInputStreamConfiguration.DEFAULT);
    S3SeekableInputStream stream = factory.createStream(s3URI, OpenStreamInformation.DEFAULT);
    byte[] singleByte = new byte[1];
    int read = stream.read(singleByte, 0, 1);
    assertEquals(1, read);
  }
}
