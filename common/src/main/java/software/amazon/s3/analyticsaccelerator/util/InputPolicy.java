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
package software.amazon.s3.analyticsaccelerator.util;

/**
 * Input policy to be used when reading an object. Useful for integrating applications to pass down
 * a policy they would like AAL to use, rather than us guessing based on object key extension.
 *
 * <p>A sequential policy means that the object will be read sequentially, regardless of the format.
 * Useful for sequential data types such as CSV, or for applications that read columnar formats
 * sequentially.
 *
 * <p>When no input policy is supplied, AAL will infer what optimisations to use based on the object
 * key extension.
 */
public enum InputPolicy {
  None,
  Sequential
}
