package com.amazon.connector.s3;

import java.io.IOException;
import java.io.InputStream;

public abstract class SeekableInputStream extends InputStream {

  public abstract void seek(long pos) throws IOException;

  public abstract long getPos();
}
