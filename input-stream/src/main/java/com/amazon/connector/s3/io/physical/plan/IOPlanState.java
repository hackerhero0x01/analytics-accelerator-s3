package com.amazon.connector.s3.io.physical.plan;

/** Enum representing a state of an IOPlan that was submitted from LogicalIO. */
public enum IOPlanState {

  /** IOPlan was successfully submitted and is executed by the physical IO */
  SUBMITTED,

  /** No IOPlan was submitted to PhysicalIO */
  SKIPPED,

  /** Failed to submit IOPlan to PhysicalIO */
  FAILED
}
