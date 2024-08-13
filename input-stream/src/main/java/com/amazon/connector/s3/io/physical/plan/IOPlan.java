package com.amazon.connector.s3.io.physical.plan;

import java.util.Iterator;
import java.util.List;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

/** A logical IO plan */
@Builder
@Getter
public class IOPlan {
  @NonNull List<Range> prefetchRanges;

  /**
   * Returns a string representation of the object.
   *
   * @return a string representation of the object.
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append('[');
    Iterator<Range> iterator = prefetchRanges.iterator();
    while (true) {
      Range range = iterator.next();
      sb.append(range);
      if (!iterator.hasNext()) return sb.append(']').toString();
      sb.append(',').append(' ');
    }
  }
}
