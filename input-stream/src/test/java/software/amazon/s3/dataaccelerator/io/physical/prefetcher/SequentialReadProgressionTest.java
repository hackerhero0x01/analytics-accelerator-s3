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
package software.amazon.s3.dataaccelerator.io.physical.prefetcher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static software.amazon.s3.dataaccelerator.util.Constants.ONE_MB;

import org.junit.jupiter.api.Test;
import software.amazon.s3.dataaccelerator.io.physical.PhysicalIOConfiguration;

public class SequentialReadProgressionTest {

  @Test
  public void test__sequentialReadProgression__asExpected() {
    // Given: a SequentialReadProgression
    SequentialReadProgression sequentialReadProgression =
        new SequentialReadProgression(PhysicalIOConfiguration.DEFAULT);

    // When & Then: size is requested for a generation --> size is correct
    assertEquals(2 * ONE_MB, sequentialReadProgression.getSizeForGeneration(0));
    assertEquals(4 * ONE_MB, sequentialReadProgression.getSizeForGeneration(1));
    assertEquals(8 * ONE_MB, sequentialReadProgression.getSizeForGeneration(2));
    assertEquals(16 * ONE_MB, sequentialReadProgression.getSizeForGeneration(3));
  }
}
