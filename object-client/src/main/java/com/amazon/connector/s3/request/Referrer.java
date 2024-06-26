package com.amazon.connector.s3.request;

import lombok.Getter;

/** Represents the referrer header to be passed in when making a request. */
public class Referrer {
  @Getter private final String range;
  @Getter private final boolean isPrefetch;

  /**
   * Construct a referrer object.
   *
   * @param range range of data requested
   * @param isPrefetch is the request on the prefetch path?
   */
  public Referrer(String range, boolean isPrefetch) {
    this.range = range;
    this.isPrefetch = isPrefetch;
  }

  @Override
  public String toString() {
    StringBuilder referrer = new StringBuilder();
    referrer.append(range);
    referrer.append(",isPrefetch=").append(isPrefetch);
    return referrer.toString();
  }
}
