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
package software.amazon.s3.analyticsaccelerator.request;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;

/**
 * This is a class to contain any stream specific information, such as what is the higher level
 * operation that this stream belongs to? Useful for the integration with S3A, it allows S3A to pass
 * in the spanId and operation name with which a stream is opened. This is used by S3A's audit
 * logging, which is of the form "3eb3c657-3e59-4f20-b484-e215c90c49f2-00000011 Executing op_open
 * with {action_http_head_request....", where 3eb3c657-3e59-4f20-b484-e215c90c49f2-00000011 is the
 * span_id for the open() operation, and ` op_open` is the operation name. See <a
 * href="https://github.com/apache/hadoop/blob/trunk/hadoop-tools/hadoop-aws/src/site/markdown/tools/hadoop-aws/auditing.md">...</a>
 * for more details.
 */
@Builder(access = AccessLevel.PUBLIC)
@Getter
public class StreamAuditContext {
  private final String spanId;
  private final String operationName;
}
