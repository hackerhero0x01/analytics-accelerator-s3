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
