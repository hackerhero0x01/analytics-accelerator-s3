package com.amazon.connector.s3.common.telemetry;

/** Syntactic sugar over {@link TelemetrySupplier} */
@FunctionalInterface
public interface OperationSupplier extends TelemetrySupplier<Operation> {}
