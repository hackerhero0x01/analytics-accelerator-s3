package com.amazon.connector.s3.blockmanager;

import com.amazon.connector.s3.common.Preconditions;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.stream.Stream;

/**
 * A circular buffer of fixed capacity. Closes its elements before removing them. Not thread-safe.
 */
public class AutoClosingCircularBuffer<T extends Closeable> implements Closeable {

  private final ArrayList<T> buffer;
  private final int capacity;
  private int oldestIndex;

  /**
   * Creates an instance of AutoClosingCircularBuffer.
   *
   * @param maxCapacity The maximum capacity of the buffer.
   */
  public AutoClosingCircularBuffer(int maxCapacity) {
    Preconditions.checkState(0 < maxCapacity, "maxCapacity should be positive");

    this.oldestIndex = 0;
    this.capacity = maxCapacity;
    this.buffer = new ArrayList<>(maxCapacity);
  }

  /**
   * Adds an element to the buffer, potentially replacing another element if the maximum capacity
   * has been reached. Calls 'close' on elements before evicting them.
   *
   * @param element The new element to add to the buffer.
   */
  public void add(T element) {
    if (buffer.size() < capacity) {
      buffer.add(element);
    } else {
      tryClose(buffer.get(oldestIndex));
      buffer.set(oldestIndex, element);
      oldestIndex = (oldestIndex + 1) % capacity;
    }
  }

  private void tryClose(T t) {
    try {
      t.close();
    } catch (IOException e) {
      throw new RuntimeException("Unable to close element of circular buffer", e);
    }
  }

  /** Returns a conventional Java stream of the underlying objects */
  public Stream<T> stream() {
    return buffer.stream();
  }

  /** Closes the buffer, freeing up all underlying resources. */
  @Override
  public void close() {
    this.buffer.stream().forEach(this::tryClose);
  }
}
