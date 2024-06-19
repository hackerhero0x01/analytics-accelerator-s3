package com.amazon.connector.s3.io.physical;

import com.amazon.connector.s3.RandomAccessReadable;
import com.amazon.connector.s3.io.logical.parquet.ColumnMappers;
import com.amazon.connector.s3.io.physical.plan.IOPlan;
import java.io.IOException;
import java.util.Set;

/** An interface defining how a logical IO layer gets hooked into Physical IO. */
public interface PhysicalIO extends RandomAccessReadable {

  /**
   * Async method capable of executing a logical IO plan.
   *
   * @param ioPlan the plan to execute asynchronously
   */
  void execute(IOPlan ioPlan) throws IOException;

  /**
   * Gets column mappers.
   *
   * @return column mappers with parquet metadata info.
   */
  ColumnMappers columnMappers();

  /**
   * Puts column mappers.
   *
   * @param columnMappers column mappers with parquet metadata.
   */
  void putColumnMappers(ColumnMappers columnMappers);

  /**
   * Adds column to list of recent columns.
   *
   * @param columnName column to be added
   */
  void addRecentColumn(String columnName);

  /**
   * Gets a list of recent columns being read.
   *
   * @return Set of recent columns being
   */
  Set<String> getRecentColumns();
}
