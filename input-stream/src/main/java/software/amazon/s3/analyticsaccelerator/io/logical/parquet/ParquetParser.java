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
package software.amazon.s3.analyticsaccelerator.io.logical.parquet;

import static software.amazon.s3.analyticsaccelerator.util.Constants.PARQUET_FOOTER_LENGTH_SIZE;
import static software.amazon.s3.analyticsaccelerator.util.Constants.PARQUET_MAGIC_STR_LENGTH;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.parquet.format.FileMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shaded.parquet.org.apache.thrift.TException;
import shaded.parquet.org.apache.thrift.protocol.TCompactProtocol;
import shaded.parquet.org.apache.thrift.protocol.TProtocol;
import shaded.parquet.org.apache.thrift.transport.TIOStreamTransport;
import shaded.parquet.org.apache.thrift.transport.TTransportException;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/** Allows for parsing a tail of a parquet file to get its FileMetadata. */
class ParquetParser {

  private static final Logger LOG = LoggerFactory.getLogger(ParquetParser.class);

  /**
   * Parses the tail of a parquet file to obtain its FileMetaData.
   *
   * @param fileTail tail bytes of parquet file to be parsed
   * @param contentLen The length of the parquet file tail to be parsed
   * @param s3URI S3 URI
   * @return FileMetaData
   * @throws IOException
   */
  public FileMetaData parseParquetFooter(ByteBuffer fileTail, int contentLen, S3URI s3URI)
      throws IOException {

    Preconditions.checkArgument(
        contentLen > PARQUET_MAGIC_STR_LENGTH + PARQUET_FOOTER_LENGTH_SIZE,
        "Specified content length is too low");

    int fileMetadataLengthIndex =
        contentLen - PARQUET_MAGIC_STR_LENGTH - PARQUET_FOOTER_LENGTH_SIZE;

    fileTail.position(fileMetadataLengthIndex);

    byte[] buff = new byte[PARQUET_FOOTER_LENGTH_SIZE];
    fileTail.get(buff, 0, PARQUET_FOOTER_LENGTH_SIZE);

    int fileMetadataLength = readIntLittleEndian(new ByteArrayInputStream(buff));
    int fileMetadataIndex = fileMetadataLengthIndex - fileMetadataLength;

    if (fileMetadataIndex < 0) {
      LOG.info(
          "Insufficient data in cached footer for {}. Required length  is {}, provided length of data is {}. Parquet optimisations will be turned off for this file. To prevent this, increase cached length using footer.caching.size",
          s3URI.getKey(),
          fileMetadataLength,
          contentLen);
      throw new IOException(
          "Insufficient data in cached footer for "
              + s3URI.getKey()
              + ". Required length  is "
              + fileMetadataLength
              + ", provided length of data is "
              + contentLen);
    }

    fileTail.position(fileMetadataIndex);
    byte[] footer = new byte[fileMetadataLength];
    fileTail.get(footer, 0, fileMetadataLength);

    try {
      FileMetaData fmd = new FileMetaData();
      fmd.read(protocol(new ByteArrayInputStream(footer)));
      return fmd;
    } catch (TException e) {
      throw new IOException("can not read FileMetaData: " + e.getMessage(), e);
    }
  }

  private static TProtocol protocol(InputStream from) throws TTransportException {
    return protocol(new TIOStreamTransport(from));
  }

  @SuppressWarnings("deprecation")
  private static org.apache.parquet.format.InterningProtocol protocol(TIOStreamTransport t) {
    return new org.apache.parquet.format.InterningProtocol(new TCompactProtocol(t));
  }

  private static int readIntLittleEndian(InputStream in) throws IOException {
    int ch1 = in.read();
    int ch2 = in.read();
    int ch3 = in.read();
    int ch4 = in.read();

    return ((ch4 << 24) + (ch3 << 16) + (ch2 << 8) + (ch1 << 0));
  }
}
