package com.amazon.connector.s3.io.physical.blockmanager;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** This class wraps a CompletableFuture<IOBlock> */
public class PrefetchIOBlock implements Closeable {

  private final long start;

  private final long end;

  private CompletableFuture<IOBlock> ioBlockCompletableFuture;

  private static final Logger LOG = LogManager.getLogger(PrefetchIOBlock.class);

  /**
   * @param start
   * @param end
   * @param ioBlockCompletableFuture
   */
  public PrefetchIOBlock(
      long start, long end, CompletableFuture<IOBlock> ioBlockCompletableFuture) {
    this.start = start;
    this.end = end;
    this.ioBlockCompletableFuture = ioBlockCompletableFuture;
  }

  /**
   * @param pos the position in the stream
   * @return true if the PrefetchIOBlock contains the pos
   */
  public boolean contains(long pos) {
    return start <= pos && pos <= end;
  }

  /**
   * @return Complete the future and returns the IOBlock. Reurns Optional.empty() if the future
   *     completed exceptionally
   */
  public Optional<IOBlock> getIOBlock() {
    Optional<IOBlock> res = Optional.empty();
    try {
      res = Optional.of(ioBlockCompletableFuture.join());
    } catch (Exception e) {
      LOG.warn("Exception while getting the prefetch IOBlock", e);
    }
    return res;
  }

  @Override
  public void close() throws IOException {
    if (ioBlockCompletableFuture.isDone()) {
      ioBlockCompletableFuture.join().close();
    }
  }
}
