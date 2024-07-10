package com.amazon.connector.s3.io.physical.plan;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

/**
 * Object representing the state of an IO Plan execution. Effectively, this will be a handle on an
 * IOPlan execution and PhysicalIO should be able to communicate with LogicalIO through this.
 */
@Builder
@Data
public class IOPlanExecution {

  /** State of the IO Plan execution */
  @NonNull private final IOPlanState state;
}
