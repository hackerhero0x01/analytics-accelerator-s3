package com.amazon.connector.s3.io.physical.v1.blockmanager;

import com.amazon.connector.s3.common.Preconditions;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A circular buffer of fixed capacity. Closes its elements before removing them. Not thread-safe.
 */
public class AutoClosingCircularBuffer<T extends Closeable> implements Closeable {

  private final List<T> buffer;
  private final int capacity;
  private int oldestIndex;

  private AtomicBoolean closed = new AtomicBoolean();

  private static final Logger LOG = LogManager.getLogger(AutoClosingCircularBuffer.class);

  /**
   * Creates an instance of AutoClosingCircularBuffer.
   *
   * @param maxCapacity The maximum capacity of the buffer.
   */
  public AutoClosingCircularBuffer(int maxCapacity) {
    Preconditions.checkState(0 < maxCapacity, "maxCapacity should be positive");

    this.oldestIndex = 0;
    this.capacity = maxCapacity;
    this.buffer = Collections.synchronizedList(new ArrayList<>(maxCapacity));
    this.closed.set(false);
  }

  /**
   * Adds an element to the buffer, potentially replacing another element if the maximum capacity
   * has been reached. Calls 'close' on elements before evicting them.
   *
   * @param element The new element to add to the buffer.
   */
  public void add(T element) throws IOException {
    if (closed.get()) {
      LOG.error("Trying to add new element, after close() was called");
    }
    synchronized (buffer) {
      if (buffer.size() < capacity) {
        buffer.add(element);
      } else {
        buffer.get(oldestIndex).close();
        buffer.set(oldestIndex, element);
        oldestIndex = (oldestIndex + 1) % capacity;
      }
    }
  }

  /**
   * Returns a conventional Java stream of the underlying objects
   *
   * @return a stream of the buffer content
   */
  protected Stream<T> stream() {
    if (closed.get()) {
      LOG.error("Trying to add new element, after close() was called");
    }
    synchronized (buffer) {
      return buffer.stream();
    }
  }

  /**
   * Finds an item in the buffer based on a predicate.
   *
   * @param predicate the predicate to use for finding the item
   * @return an Optional containing the item if found, otherwise an empty Optional
   */
  public Optional<T> findItem(Predicate<T> predicate) {
    synchronized (buffer) {
      return buffer.stream().filter(predicate).findFirst();
    }
  }

  /** Closes the buffer, freeing up all underlying resources. */
  @Override
  public void close() throws IOException {
    closed.set(true);
    synchronized (buffer) {
      for (T t : buffer) {
        t.close();
      }
      buffer.clear();
    }
  }
}
