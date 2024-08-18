package com.amazon.connector.s3.plan;

import static org.junit.jupiter.api.Assertions.*;

import com.amazon.connector.s3.io.physical.plan.IOPlan;
import com.amazon.connector.s3.io.physical.plan.Range;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Collection;
import org.junit.jupiter.api.Test;

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
    assertEquals("[[1-2], [10-20]]", ioPlan.toString());
  }

  @Test
  void testRangeToString() {
    IOPlan ioPlan = new IOPlan(new Range(1, 2));
    assertEquals("[[1-2]]", ioPlan.toString());
  }

  @Test
  void testEmptyPlanToString() {
    assertEquals("[]", IOPlan.EMPTY_PLAN.toString());
  }
}
