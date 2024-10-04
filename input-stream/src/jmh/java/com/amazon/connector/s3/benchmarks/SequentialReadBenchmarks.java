package com.amazon.connector.s3.benchmarks;

import com.amazon.connector.s3.benchmarks.common.BenchmarkObject;

/** Sequential read benchmarks */
public class SequentialReadBenchmarks {
  /** Runs the sequential benchmark for 1MB object */
  public static class Seq_Read_1MB extends SequentialRead {
    /** Constructs the benchmark */
    public Seq_Read_1MB() {
      super(BenchmarkObject.RANDOM_1MB);
    }
  }
  /** Runs the sequential benchmark for 4MB object */
  public static class Seq_Read_4MB extends SequentialRead {
    /** Constructs the benchmark */
    public Seq_Read_4MB() {
      super(BenchmarkObject.RANDOM_4MB);
    }
  }
  /** Runs the sequential benchmark for 16MB object */
  public static class Seq_Read_16MB extends SequentialRead {
    /** Constructs the benchmark */
    public Seq_Read_16MB() {
      super(BenchmarkObject.RANDOM_16MB);
    }
  }
  /** Runs the sequential benchmark for 64MB object */
  public static class Seq_Read_64MB extends SequentialRead {
    /** Constructs the benchmark */
    public Seq_Read_64MB() {
      super(BenchmarkObject.RANDOM_64MB);
    }
  }
  /** Runs the sequential benchmark for 128MB object */
  public static class Seq_Read_128MB extends SequentialRead {
    /** Constructs the benchmark */
    public Seq_Read_128MB() {
      super(BenchmarkObject.RANDOM_128MB);
    }
  }
}
