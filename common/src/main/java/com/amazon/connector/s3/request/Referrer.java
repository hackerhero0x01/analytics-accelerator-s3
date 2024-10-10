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
package com.amazon.connector.s3.request;

import lombok.Value;

/** Represents the referrer header to be passed in when making a request. */
@Value
public class Referrer {
  private final String range;
  private final ReadMode readMode;

  /**
   * Construct a referrer object.
   *
   * @param range range of data requested
   * @param readMode is this a sync or async read?
   */
  public Referrer(String range, ReadMode readMode) {
    this.range = range;
    this.readMode = readMode;
  }

  @Override
  public String toString() {
    StringBuilder referrer = new StringBuilder();
    referrer.append(range);
    referrer.append(",readMode=").append(readMode);
    return referrer.toString();
  }
}
