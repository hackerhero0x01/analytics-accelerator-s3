package com.amazon.connector.s3.plan;

import static org.junit.jupiter.api.Assertions.*;

import com.amazon.connector.s3.io.physical.plan.IOPlan;
import com.amazon.connector.s3.io.physical.plan.Range;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;

public class IOPlanTest {
  @Test
  void testConstructor() {
    ArrayList<Range> ranges = new ArrayList<>();
    ranges.add(new Range(1, 2));
    ranges.add(new Range(10, 20));
    IOPlan ioPlan = IOPlan.builder().prefetchRanges(ranges).build();
    assertArrayEquals(ioPlan.getPrefetchRanges().toArray(), ranges.toArray());
  }

  @Test
  void testConstructorThrowOnNulls() {
    assertThrows(NullPointerException.class, () -> IOPlan.builder().prefetchRanges(null));
  }

  @Test
  void testToString() {
    ArrayList<Range> ranges = new ArrayList<>();
    ranges.add(new Range(1, 2));
    ranges.add(new Range(10, 20));
    IOPlan ioPlan = IOPlan.builder().prefetchRanges(ranges).build();
    assertEquals("[[1-2], [10-20]]", ioPlan.toString());
  }
}
