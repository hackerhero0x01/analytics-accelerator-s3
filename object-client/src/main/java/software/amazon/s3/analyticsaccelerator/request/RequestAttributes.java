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

import software.amazon.awssdk.core.interceptor.ExecutionAttribute;

/**
 * These execution attributes are specific to the hadoop S3A integration, and are required for S3A's auditing feature.
 * These execution attributes can be set per request, which are then picked up by execution interceptors in S3A.
 * See S3A's LoggingAuditor for more usage.
 */
public class RequestAttributes
{
    public static final ExecutionAttribute<String> SPAN_ID  = new ExecutionAttribute<>("span");
    public static final ExecutionAttribute<String> OPERATION_NAME  = new ExecutionAttribute<>("operation");
}
