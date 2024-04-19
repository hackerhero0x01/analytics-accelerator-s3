package com.amazon.connector.s3;

import java.io.InputStream;

public abstract class SeekableInputStream extends InputStream {

  public abstract void seek(long pos);

  public abstract long getPos();
}
