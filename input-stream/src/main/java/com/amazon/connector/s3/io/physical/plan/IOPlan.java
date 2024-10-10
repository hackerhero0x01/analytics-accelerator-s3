  /*
   * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
   *
   * Licensed under the Apache License, Version 2.0 (the "License").
   * You may not use this file except in compliance with the License.
   * You may obtain a copy of the License at
   *
   *   http://www.apache.org/licenses/LICENSE-2.0
  
   * Unless required by applicable law or agreed to in writing, software
   * distributed under the License is distributed on an "AS IS" BASIS,
   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   * See the License for the specific language governing permissions and
   * limitations under the License.
   */
package com.amazon.connector.s3.io.physical.plan;

import com.amazon.connector.s3.request.Range;
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
