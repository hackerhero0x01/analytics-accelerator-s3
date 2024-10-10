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
package com.amazon.connector.s3.util;

import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import java.util.regex.Pattern;

/** A LogicalIO factory based on S3URI file extensions. */
public class ObjectFormatSelector {

  private final Pattern parquetPattern;

  /**
   * Creates a new instance of {@ObjectFormatSelector}. Used to select the file format of a
   * particular object key.
   *
   * @param configuration LogicalIO configuration.
   */
  public ObjectFormatSelector(LogicalIOConfiguration configuration) {
    this.parquetPattern =
        Pattern.compile(configuration.getParquetFormatSelectorRegex(), Pattern.CASE_INSENSITIVE);
  }

  /**
   * Uses a regex matcher to select the file format based on the file extension of the key.
   *
   * @param s3URI the object's S3 URI
   * @return the file format of the object
   */
  public ObjectFormat getObjectFormat(S3URI s3URI) {
    if (parquetPattern.matcher(s3URI.getKey()).find()) {
      return ObjectFormat.PARQUET;
    }

    return ObjectFormat.DEFAULT;
  }
}
