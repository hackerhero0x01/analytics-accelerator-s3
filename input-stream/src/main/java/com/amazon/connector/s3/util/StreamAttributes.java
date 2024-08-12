package com.amazon.connector.s3.util;

import com.amazon.connector.s3.common.telemetry.Attribute;
import com.amazon.connector.s3.request.Range;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum StreamAttributes {
    URI("uri"),
    RANGE("range");
    private final String name;

    /**
     * Creates a and {@link Attribute} for a {@link S3URI}.
     *
     * @param s3URI the {@link S3URI} to create the attribute from.
     * @return The new instance of the {@link Attribute}.
     */
    public static Attribute uriAttribute(S3URI s3URI) {
        return Attribute.of(StreamAttributes.URI.getName(), s3URI.toString());
    }

    /**
     * Creates a and {@link Attribute} for a {@link Range}.
     *
     * @param range the {@link Range} to create the attribute from.
     * @return The new instance of the {@link Attribute}.
     */
    public static Attribute rangeAttribute(Range range) {
        return Attribute.of(StreamAttributes.RANGE.getName(), range.toString());
    }
}
