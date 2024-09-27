package com.amazon.connector.s3.common.telemetry;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Value;

/**
 * Telemetry operation. An operation is a template for a specific execution and is defined by a name
 * and a set of attributes, as well as other context, such as parent operation and context. It
 * doesn't carry the actual duration - that is defined by a specific instance of a metric,
 * represented by {@link OperationMeasurement}
 */
// Implementation note: the builder is implemented by hand, as opposed to via Lombok to create more
// streamlined syntax for attribute specification
@Value
@EqualsAndHashCode(callSuper = true)
public class Operation extends TelemetryDatapoint {
  /** Operation ID. Must not be null. */
  @NonNull String id;

  /** Parent operation. Optional. */
  @NonNull Optional<Operation> parent;

  /** Operation Context */
  @NonNull OperationContext context;

  /**
   * Creates a new instance of {@link Operation}.
   *
   * @param id operation id.
   * @param name operation name.
   * @param attributes operation attributes.
   * @param context operation context.
   * @param parent operation parent.
   */
  Operation(
      String id,
      String name,
      Map<String, Attribute> attributes,
      OperationContext context,
      Optional<Operation> parent) {
    // Set the parent automatically in the constructor called by the builder
    this(id, name, attributes, context, parent, true);
  }

  /**
   * Creates a new instance of {@link Operation}.
   *
   * @param id operation id.
   * @param name operation name.
   * @param attributes operation attributes.
   * @param context operation context.
   * @param parent operation parent.
   * @param inferParent - whether to set the parent automatically.
   */
  Operation(
      @NonNull String id,
      @NonNull String name,
      @NonNull Map<String, Attribute> attributes,
      @NonNull OperationContext context,
      @NonNull Optional<Operation> parent,
      boolean inferParent) {
    super(name, addStandardAttributes(attributes));
    this.id = id;
    this.context = context;
    // If the parent is not supplied, and we are allowed to infer it, use the OperationContext
    if (!parent.isPresent() && inferParent) {
      this.parent = this.context.getCurrentNonDefaultOperation();
    } else {
      this.parent = parent;
    }
  }

  /**
   * Adds standard attributes to the {@link Operation attributes}.
   *
   * @param attributes provided attributes.
   * @return an updated collection of attributes.
   */
  private static Map<String, Attribute> addStandardAttributes(Map<String, Attribute> attributes) {
    Attribute threadIdAttribute = CommonAttributes.threadId(Thread.currentThread());
    attributes.put(threadIdAttribute.getName(), threadIdAttribute);

    return attributes;
  }

  /**
   * Creates a builder for {@link Operation}
   *
   * @return a new instance of {@link OperationBuilder}
   */
  public static OperationBuilder builder() {
    return new OperationBuilder();
  }

  /**
   * Returns the String representation of the {@link Operation}.
   *
   * @return the String representation of the {@link Operation}.
   */
  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    // id
    stringBuilder.append("[");
    stringBuilder.append(id);

    // add parent id, if present
    if (parent.isPresent()) {
      stringBuilder.append("<-");
      stringBuilder.append(parent.get().getId());
    }
    stringBuilder.append("] ");

    // name
    stringBuilder.append(this.getName());

    // attributes
    appendAttributes(stringBuilder);

    return stringBuilder.toString();
  }

  /** Builder for {@link Operation} */
  public static class OperationBuilder
      extends TelemetryDatapointBuilder<Operation, OperationBuilder> {
    private String id;
    private Optional<Operation> parent = Optional.empty();
    private OperationContext context = OperationContext.DEFAULT;

    private static final int ID_GENERATION_RADIX = 32;

    /**
     * Sets the operation id. if not used, a new ID will be generated
     *
     * @param id operation id. Must not be null.
     * @return the current instance of {@link OperationBuilder}.
     */
    public OperationBuilder id(@NonNull String id) {
      this.id = id;
      return this;
    }

    /**
     * Sets the operation parent
     *
     * @param parent operation parent. Must not be null.
     * @return the current instance of {@link OperationBuilder}.
     */
    public OperationBuilder parent(@NonNull Operation parent) {
      this.parent = Optional.of(parent);
      return this;
    }

    /**
     * Sets the operation context
     *
     * @param context operation parent. Must not be null.
     * @return the current instance of {@link OperationContext}.
     */
    public OperationBuilder context(@NonNull OperationContext context) {
      this.context = context;
      return this;
    }

    /**
     * Builds the instance of {@link Operation}.
     *
     * @return a new instance of {@link Operation}
     */
    @Override
    protected Operation buildCore() {
      // generate ID if needed
      if (this.id == null) {
        this.id = generateID();
      }
      // Create the operation
      return new Operation(
          this.id, this.getName(), this.getAttributes(), this.context, this.parent);
    }

    /**
     * This generates a unique ID. The implementation uses Base64 encoding of a random Long
     *
     * @return unique id.
     */
    private String generateID() {
      // Generate a random long and encode it with high radix for compactness
      // Here we are using 32 bit radix, as it provides a good balance between
      // performant generation and compaction
      long random = ThreadLocalRandom.current().nextLong();
      return Long.toString(random, ID_GENERATION_RADIX);
    }
  }
}
