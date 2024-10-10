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
