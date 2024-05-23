package com.amazon.connector.s3.io.physical;

import com.amazon.connector.s3.RandomAccessReadable;
import com.amazon.connector.s3.io.physical.plan.IOPlan;
import java.io.IOException;

/** An interface defining how a logical IO layer gets hooked into Physical IO. */
public interface PhysicalIO extends RandomAccessReadable {

  /**
   * Async method capable of executing a logical IO plan.
   *
   * @param ioPlan the plan to execute asynchronously
   */
  void execute(IOPlan ioPlan) throws IOException;
}
