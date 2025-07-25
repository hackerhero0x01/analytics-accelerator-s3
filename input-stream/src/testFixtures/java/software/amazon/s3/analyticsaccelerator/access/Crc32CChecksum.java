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
package software.amazon.s3.analyticsaccelerator.access;

/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import static software.amazon.awssdk.core.internal.util.HttpChecksumUtils.longToByte;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.zip.Checksum;
import software.amazon.awssdk.core.internal.checksums.factory.CrtBasedChecksumProvider;
import software.amazon.awssdk.core.internal.checksums.factory.SdkCrc32C;

/**
 * Implementation of CRC32C checksum algorithm for data integrity verification. This class provides
 * methods to calculate and manage CRC32C checksums, supporting both CRT-based implementation (when
 * available) and SDK-based implementation as fallback.
 */
public class Crc32CChecksum {

  /** The current checksum calculator instance */
  private Checksum crc32c;

  /** Saved checksum state for mark/reset functionality */
  private Checksum lastMarkedCrc32C;

  /** Flag indicating whether we're using the CRT-based implementation */
  private final boolean isCrtBasedChecksum;

  /**
   * Creates CRT Based Crc32C checksum if Crt classpath for Crc32c is loaded, else create Sdk
   * Implemented Crc32c
   */
  public Crc32CChecksum() {
    crc32c = CrtBasedChecksumProvider.createCrc32C();
    isCrtBasedChecksum = crc32c != null;
    if (!isCrtBasedChecksum) {
      crc32c = SdkCrc32C.create();
    }
  }

  /**
   * Returns the current checksum value as a byte array.
   *
   * @return A 4-byte array containing the CRC32C checksum
   */
  public byte[] getChecksumBytes() {
    return Arrays.copyOfRange(longToByte(crc32c.getValue()), 4, 8);
  }

  /**
   * Marks the current position in the checksum calculation for later reset.
   *
   * @param readLimit Ignored, included for compatibility with InputStream mark method
   */
  public void mark(int readLimit) {
    this.lastMarkedCrc32C = cloneChecksum(crc32c);
  }

  /**
   * Updates the checksum with the specified byte.
   *
   * @param b The byte to update the checksum with
   */
  public void update(int b) {
    crc32c.update(b);
  }

  /**
   * Updates the checksum with the specified array of bytes.
   *
   * @param b The array of bytes to update the checksum with
   * @param off The offset into the array where the update should start
   * @param len The number of bytes to use for the update
   */
  public void update(byte[] b, int off, int len) {
    crc32c.update(b, off, len);
  }

  /**
   * Returns the current checksum value.
   *
   * @return The current checksum value as a long
   */
  public long getValue() {
    return crc32c.getValue();
  }

  /** Resets the checksum to the last marked position, or to the initial state if no mark exists. */
  public void reset() {
    if (lastMarkedCrc32C == null) {
      crc32c.reset();
    } else {
      crc32c = cloneChecksum(lastMarkedCrc32C);
    }
  }

  /**
   * Creates a clone of the given checksum object. Uses reflection for CRT-based checksums and
   * direct cloning for SDK-based checksums.
   *
   * @param checksum The checksum to clone
   * @return A clone of the provided checksum
   * @throws IllegalStateException If cloning fails
   */
  private Checksum cloneChecksum(Checksum checksum) {
    if (isCrtBasedChecksum) {
      try {
        Method method = checksum.getClass().getDeclaredMethod("clone");
        return (Checksum) method.invoke(checksum);
      } catch (ReflectiveOperationException e) {
        throw new IllegalStateException("Could not clone checksum class " + checksum.getClass(), e);
      }
    } else {
      return (Checksum) ((SdkCrc32C) checksum).clone();
    }
  }
}
