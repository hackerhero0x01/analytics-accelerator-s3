/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
