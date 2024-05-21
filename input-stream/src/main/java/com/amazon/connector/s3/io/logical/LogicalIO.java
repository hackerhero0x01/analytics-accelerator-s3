package com.amazon.connector.s3.io.logical;

import com.amazon.connector.s3.RandomAccessReadable;

/**
 * Class responsible for implementing "logical" reads. Logical reads are not concerned with the
 * "how" of reading data but only with "what" data to read.
 *
 * <p>Having these method present as a separate interface allows an implementation to "intercept"
 * these method invocations and schedule async jobs by interacting with a physical layer.
 */
public interface LogicalIO extends RandomAccessReadable {

  /**
   * Seek TODO: proper description here talks about top level interface not being
   * randomaccessreadable, this being position aware but physical io not being position aware
   */
  void seek(long newPos);

  // TODO: Fill this in.

}
