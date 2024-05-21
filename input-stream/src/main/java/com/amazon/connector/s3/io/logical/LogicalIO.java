package com.amazon.connector.s3.io.logical;

import com.amazon.connector.s3.RandomAccessReadable;

/**
 * Interface responsible for implementing "logical" reads. Logical reads are not concerned with the
 * "how" of reading data but only with "what" data to read.
 *
 * <p>On a high level the logical IO layer sits above a physical IO layer and "observes" what data
 * is being read by the user calling the stream. Based on this history and other hints (such as key
 * name, metadata information) the logical IO layer can formulate "what" data should be read. The
 * logical layer should be able to create an IOPlan based on this and use the physical layer to
 * execute this asynchronously.
 *
 * <p>The logical IO layer must be aware of seeks as it listens to all high-level IO actions to
 * implement a history.
 */
public interface LogicalIO extends RandomAccessReadable {

  /**
   * Seek to a new position.
   *
   * @param newPos the new position in the stream
   */
  void seek(long newPos);
}
