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
/**
 * The StreamContext interface provides methods for modifying and building referrer header which
 * will then be attached to subsequent HTTP requests.
 */
public interface StreamContext {

  /**
   * Modifies and builds the referrer header string for a given request context.
   *
   * <p>Implementation Note: To ensure thread safety, implementations should create and modify a
   * copy of the internal state rather than modifying the original object directly. This is crucial
   * as multiple threads may be accessing the same StreamContext instance concurrently.
   *
   * <p>Example implementation:
   *
   * <pre>
   * public class S3AAuditHeaders implements AuditHeaders {
   *     private final HttpReferrerAuditHeader referrer;
   *
   *     public S3AAuditHeaders(HttpReferrerAuditHeader referrer) {
   *         this.referrer = referrer;
   *     }
   *
   *     &#64;Override
   *     public String modifyAndBuildReferrerHeader(GetRequest getRequestContext) {
   *         // Create a copy to ensure thread safety
   *         HttpReferrerAuditHeader copyReferrer = new HttpReferrerAuditHeader(this.referrer);
   *         copyReferrer.set(AuditConstants.PARAM_RANGE, getRequestContext.getRange().toHttpString());
   *         return copyReferrer.buildHttpReferrer();
   *     }
   * }
   * </pre>
   *
   * @param getRequestContext the request context for building the referrer header
   * @return the modified and built referrer header as a String
   */
  public String modifyAndBuildReferrerHeader(GetRequest getRequestContext);
}
