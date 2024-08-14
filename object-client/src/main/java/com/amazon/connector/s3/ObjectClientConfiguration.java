package com.amazon.connector.s3;

import com.amazon.connector.s3.common.ConnectorConfiguration;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/** Configuration for {@link ObjectClient} */
@Getter
@Builder
@EqualsAndHashCode
public class ObjectClientConfiguration {

  private static final String DEFAULT_USER_AGENT_PREFIX = null;

  /** User Agent Prefix. {@link ObjectClientConfiguration#DEFAULT_USER_AGENT_PREFIX} by default. */
  @Builder.Default private String userAgentPrefix = DEFAULT_USER_AGENT_PREFIX;

  private static final String USER_AGENT_PREFIX_KEY = "useragentprefix";

  public static final ObjectClientConfiguration DEFAULT =
      ObjectClientConfiguration.builder().build();

  /**
   * Constructs {@link ObjectClientConfiguration} from {@link ConnectorConfiguration} object.
   *
   * @param configuration Configuration object to generate ObjectClientConfiguration from
   * @return ObjectClientConfiguration
   */
  public static ObjectClientConfiguration fromConfiguration(ConnectorConfiguration configuration) {
    return ObjectClientConfiguration.builder()
        .userAgentPrefix(configuration.getString(USER_AGENT_PREFIX_KEY, DEFAULT_USER_AGENT_PREFIX))
        .build();
  }

  /**
   * Construct {@link ObjectClientConfiguration}
   *
   * @param userAgentPrefix Prefix to prepend to ObjectClient's userAgent.
   */
  @Builder
  private ObjectClientConfiguration(String userAgentPrefix) {
    this.userAgentPrefix = userAgentPrefix;
  }
}
