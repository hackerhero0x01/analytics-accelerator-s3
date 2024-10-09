package com.amazon.connector.s3.access;

import com.amazon.connector.s3.S3SeekableInputStreamConfiguration;
import lombok.AllArgsConstructor;
import lombok.Getter;

/** Enum representing meaningful configuration samples for {@link S3ExecutionConfiguration} */
@AllArgsConstructor
@Getter
public enum DATInputStreamConfigurationKind {
  DEFAULT("DEFAULT", S3SeekableInputStreamConfiguration.DEFAULT);

  private final String name;
  private final S3SeekableInputStreamConfiguration value;
}
