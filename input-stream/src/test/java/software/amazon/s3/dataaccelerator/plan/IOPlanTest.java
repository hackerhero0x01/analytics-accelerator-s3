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
package software.amazon.s3.dataaccelerator.plan;

import static org.junit.jupiter.api.Assertions.*;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Collection;
import org.junit.jupiter.api.Test;
import software.amazon.s3.dataaccelerator.io.physical.plan.IOPlan;
import software.amazon.s3.dataaccelerator.request.Range;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class IOPlanTest {
  @Test
  void testRangeListConstructor() {
    ArrayList<Range> ranges = new ArrayList<>();
    ranges.add(new Range(1, 2));
    ranges.add(new Range(10, 20));
    IOPlan ioPlan = new IOPlan(ranges);
    assertArrayEquals(ioPlan.getPrefetchRanges().toArray(), ranges.toArray());
  }

  @Test
  void testRangeConstructor() {
    IOPlan ioPlan = new IOPlan(new Range(1, 2));
    assertArrayEquals(ioPlan.getPrefetchRanges().toArray(), new Range[] {new Range(1, 2)});
  }

  @Test
  void testConstructorThrowOnNulls() {
    assertThrows(NullPointerException.class, () -> new IOPlan((Collection<Range>) null));
    assertThrows(NullPointerException.class, () -> new IOPlan((Range) null));
  }

  @Test
  void testRangeListToString() {
    ArrayList<Range> ranges = new ArrayList<>();
    ranges.add(new Range(1, 2));
    ranges.add(new Range(10, 20));
    IOPlan ioPlan = new IOPlan(ranges);
    assertEquals("[1-2,10-20]", ioPlan.toString());
  }

  @Test
  void testRangeToString() {
    IOPlan ioPlan = new IOPlan(new Range(1, 2));
    assertEquals("[1-2]", ioPlan.toString());
  }

  @Test
  void testEmptyPlanToString() {
    assertEquals("[]", IOPlan.EMPTY_PLAN.toString());
  }
}
