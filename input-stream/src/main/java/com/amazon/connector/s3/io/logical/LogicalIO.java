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
 * <p>For now, this interface is a marker interface but should become more soon.
 */
public interface LogicalIO extends RandomAccessReadable {}
