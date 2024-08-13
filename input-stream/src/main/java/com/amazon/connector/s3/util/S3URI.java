package com.amazon.connector.s3.util;

import java.net.URI;
import lombok.NonNull;
import lombok.Value;

/** Container for representing an 's3://' or 's3a://'-style S3 location. */
@Value(staticConstructor = "of")
public class S3URI {
  @NonNull String bucket;
  @NonNull String key;

  private static String URI_FORMAT_STRING = "%s://%s/%s";
  private static String URI_SCHEME_DEFAULT = "s3";

  /**
   * Creates the {@link URI} corresponding to this {@link S3URI}.
   *
   * @return the newly created {@link S3URI}
   */
  public URI toURI() {
    return toURI(URI_SCHEME_DEFAULT);
  }

  /**
   * Creates the {@link URI} corresponding to this {@link S3URI}.
   *
   * @param scheme URI scheme to use
   * @return the newly created {@link S3URI}
   */
  public URI toURI(String scheme) {
    return URI.create(this.toString(scheme));
  }

  /**
   * Creates the string representation of the {@link S3URI}.
   *
   * @return the string representation of the {@link URI}
   */
  @Override
  public String toString() {
    return toString(URI_SCHEME_DEFAULT);
  }

  /**
   * Creates the string representation of the {@link S3URI}.
   *
   * @param scheme URI scheme to use
   * @return the string representation of the {@link URI}
   */
  public String toString(@NonNull String scheme) {
    return String.format(URI_FORMAT_STRING, scheme, this.getBucket(), this.getKey());
  }
}
