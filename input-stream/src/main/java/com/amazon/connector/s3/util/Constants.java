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

/** S3SeekableInputStream constants. */
public final class Constants {
  /** Prevent instantiation, this is meant to be a facade */
  private Constants() {}

  public static final int ONE_KB = 1024;
  public static final int ONE_MB = 1024 * 1024;
  public static final int PARQUET_MAGIC_STR_LENGTH = 4;
  public static final int PARQUET_FOOTER_LENGTH_SIZE = 4;
}
