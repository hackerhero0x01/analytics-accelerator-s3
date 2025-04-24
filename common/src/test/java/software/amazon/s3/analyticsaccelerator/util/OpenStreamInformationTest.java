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
package software.amazon.s3.analyticsaccelerator.util;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.StreamContext;

public class OpenStreamInformationTest {

  @Test
  public void testDefaultInstance() {
    OpenStreamInformation info = OpenStreamInformation.DEFAULT;

    assertNotNull(info, "Default instance should not be null");
    assertNull(info.getStreamContext(), "Default streamContext should be null");
    assertNull(info.getObjectMetadata(), "Default objectMetadata should be null");
    assertNull(info.getInputPolicy(), "Default inputPolicy should be null");
  }

  @Test
  public void testBuilderWithAllFields() {
    StreamContext mockContext = Mockito.mock(StreamContext.class);
    ObjectMetadata mockMetadata = Mockito.mock(ObjectMetadata.class);
    InputPolicy mockPolicy = Mockito.mock(InputPolicy.class);

    OpenStreamInformation info =
        OpenStreamInformation.builder()
            .streamContext(mockContext)
            .objectMetadata(mockMetadata)
            .inputPolicy(mockPolicy)
            .build();

    assertSame(mockContext, info.getStreamContext(), "StreamContext should match");
    assertSame(mockMetadata, info.getObjectMetadata(), "ObjectMetadata should match");
    assertSame(mockPolicy, info.getInputPolicy(), "InputPolicy should match");
  }

  @Test
  public void testBuilderWithPartialFields() {
    StreamContext mockContext = Mockito.mock(StreamContext.class);
    ObjectMetadata mockMetadata = Mockito.mock(ObjectMetadata.class);

    OpenStreamInformation info =
        OpenStreamInformation.builder()
            .streamContext(mockContext)
            .objectMetadata(mockMetadata)
            .build();

    assertSame(mockContext, info.getStreamContext(), "StreamContext should match");
    assertSame(mockMetadata, info.getObjectMetadata(), "ObjectMetadata should match");
    assertNull(info.getInputPolicy(), "InputPolicy should be null");
  }

  @Test
  public void testBuilderFieldRetention() {
    // Create mocks
    StreamContext mockContext = Mockito.mock(StreamContext.class);
    ObjectMetadata mockMetadata = Mockito.mock(ObjectMetadata.class);
    InputPolicy mockPolicy = Mockito.mock(InputPolicy.class);

    // Build object
    OpenStreamInformation info =
        OpenStreamInformation.builder()
            .streamContext(mockContext)
            .objectMetadata(mockMetadata)
            .inputPolicy(mockPolicy)
            .build();

    // Verify field retention
    assertNotNull(info, "Built object should not be null");
    assertNotNull(info.getStreamContext(), "StreamContext should be retained");
    assertNotNull(info.getObjectMetadata(), "ObjectMetadata should be retained");
    assertNotNull(info.getInputPolicy(), "InputPolicy should be retained");
  }

  @Test
  public void testNullFields() {
    OpenStreamInformation info =
        OpenStreamInformation.builder()
            .streamContext(null)
            .objectMetadata(null)
            .inputPolicy(null)
            .build();

    assertNull(info.getStreamContext(), "StreamContext should be null");
    assertNull(info.getObjectMetadata(), "ObjectMetadata should be null");
    assertNull(info.getInputPolicy(), "InputPolicy should be null");
  }
}
