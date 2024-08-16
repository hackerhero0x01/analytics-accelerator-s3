package com.amazon.connector.s3.io.physical.plan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import lombok.Getter;
import lombok.NonNull;

/** A logical IO plan */
@Getter
public class IOPlan {
  private final ArrayList<Range> prefetchRanges;
  public static final IOPlan EMPTY_PLAN = new IOPlan(Collections.emptyList());

  /**
   * Creates a new instance of {@link IOPlan}
   *
   * @param prefetchRange single prefetch range
   */
  public IOPlan(@NonNull Range prefetchRange) {
    this.prefetchRanges = new ArrayList<>(1);
    this.prefetchRanges.add(prefetchRange);
  }
  /**
   * Creates a new instance of {@link IOPlan}
   *
   * @param prefetchRanges prefetch ranges
   */
  public IOPlan(@NonNull Collection<Range> prefetchRanges) {
    this.prefetchRanges = new ArrayList<>(prefetchRanges);
  }

  /**
   * Returns a string representation of the object.
   *
   * @return a string representation of the object.
   */
  @Override
  public String toString() {
    return this.prefetchRanges.toString();
  }
}
