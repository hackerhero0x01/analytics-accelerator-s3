package com.amazon.connector.s3.common.telemetry;

import com.amazon.connector.s3.common.Preconditions;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import lombok.NonNull;
import lombok.Value;

/**
 * Telemetry operation. This represents an execution of an operation. An operation is defined by
 * name and a set of attributes.
 */
// Implementation note: the builder is implemented by hand, as opposed to via Lombok to create more
// streamlined syntax for attribute specification
@Value
public class Operation {
  /** Operation ID. Must not be null. */
  @NonNull String id;

  /** Operation name. Must not be null. */
  @NonNull String name;

  /** Operation attributes */
  @NonNull Map<String, Attribute> attributes;

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
    this.id = id;
    this.name = name;
    this.context = context;
    this.attributes = Collections.unmodifiableMap(addStandardAttributes(attributes));
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
    Attribute threadIdAttribute = CommonAttributes.threadIdAttribute(Thread.currentThread());
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
    StringBuilder builder = new StringBuilder();
    // id
    builder.append("[");
    builder.append(id);

    // add parent id, if present
    if (parent.isPresent()) {
      builder.append("<-");
      builder.append(parent.get().getId());
    }
    builder.append("] ");

    // name
    builder.append(name);

    // attributes
    if (!attributes.isEmpty()) {
      builder.append("(");
      int count = 0;
      for (Attribute attribute : attributes.values()) {
        builder.append(attribute.getName());
        builder.append("=");
        builder.append(attribute.getValue());
        if (++count != attributes.size()) {
          builder.append(", ");
        }
      }
      builder.append(")");
    }

    return builder.toString();
  }

  /** Builder for {@link Operation} */
  public static class OperationBuilder {
    private static final int ID_BYTE_SIZE = 8;
    private static final int ID_STRING_SIZE = 11;
    private String id;
    private String name;
    private final Map<String, Attribute> attributes = new HashMap<String, Attribute>();
    private Optional<Operation> parent = Optional.empty();
    private OperationContext context = OperationContext.DEFAULT;

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
     * Sets the operation name.
     *
     * @param name operation name. Must not be null.
     * @return the current instance of {@link OperationBuilder}.
     */
    public OperationBuilder name(@NonNull String name) {
      this.name = name;
      return this;
    }

    /**
     * Adds a new attribute to the operation.
     *
     * @param name attribute name. Must not be null.
     * @param value attribute value. Must not be null.
     * @return the current instance of {@link OperationBuilder}.
     */
    public OperationBuilder attribute(@NonNull String name, @NonNull Object value) {
      return attribute(Attribute.of(name, value));
    }

    /**
     * Adds a new attribute to the operation.
     *
     * @param attribute attribute to add.
     * @return the current instance of {@link OperationBuilder}.
     */
    public OperationBuilder attribute(@NonNull Attribute attribute) {
      // Add the attribute, presuming the is no another attribute by that name
      Attribute existingAttribute = this.attributes.putIfAbsent(attribute.getName(), attribute);
      if (existingAttribute != null) {
        throw new IllegalArgumentException(
            "Attribute with this name already already exists: " + existingAttribute);
      }
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
    public Operation build() {
      // Check name - the rest has sensible defaults
      Preconditions.checkNotNull(this.name, "Operation name cannot must be specified");

      // generate ID if needed
      if (this.id == null) {
        this.id = generateID();
      }

      // Create the operation
      return new Operation(this.id, this.name, this.attributes, this.context, this.parent);
    }

    /**
     * This generates a unique ID. The implementation uses Base64 encoding of a random Long
     *
     * @return unique id.
     */
    private String generateID() {
      // Generate a random long and base64 it, stripping the trailing padding
      byte[] buffer = new byte[8];
      ThreadLocalRandom.current().nextBytes(buffer);
      return Base64.getUrlEncoder().encodeToString(buffer).substring(0, ID_STRING_SIZE);
    }
  }
}
