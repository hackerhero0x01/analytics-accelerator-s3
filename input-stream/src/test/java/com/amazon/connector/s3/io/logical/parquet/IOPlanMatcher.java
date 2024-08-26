package com.amazon.connector.s3.io.logical.parquet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import com.amazon.connector.s3.io.physical.plan.IOPlan;
import com.amazon.connector.s3.request.Range;
import java.util.List;
import org.mockito.ArgumentMatcher;

class IOPlanMatcher implements ArgumentMatcher<IOPlan> {
  private final List<Range> expectedRanges;

  public IOPlanMatcher(List<Range> expectedRanges) {
    this.expectedRanges = expectedRanges;
  }

  @Override
  public boolean matches(IOPlan argument) {
    assertArrayEquals(argument.getPrefetchRanges().toArray(), expectedRanges.toArray());
    return true;
  }
}
