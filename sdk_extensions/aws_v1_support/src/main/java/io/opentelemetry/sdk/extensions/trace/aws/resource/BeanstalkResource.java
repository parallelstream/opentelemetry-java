/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extensions.trace.aws.resource;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.common.Attributes;
import io.opentelemetry.sdk.resources.ResourceAttributes;
import io.opentelemetry.sdk.resources.ResourceProvider;
import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A {@link ResourceProvider} which provides information about the current EC2 instance if running
 * on AWS Elastic Beanstalk.
 */
public class BeanstalkResource extends ResourceProvider {

  private static final Logger logger = Logger.getLogger(BeanstalkResource.class.getName());

  private static final String DEVELOPMENT_ID = "deployment_id";
  private static final String VERSION_LABEL = "version_label";
  private static final String ENVIRONMENT_NAME = "environment_name";
  private static final String BEANSTALK_CONF_PATH = "/var/elasticbeanstalk/xray/environment.conf";
  private static final JsonFactory JSON_FACTORY = new JsonFactory();

  private final String configPath;

  /**
   * Returns a {@link BeanstalkResource} which attempts to compute information about the Beanstalk
   * environment if available.
   */
  public BeanstalkResource() {
    this(BEANSTALK_CONF_PATH);
  }

  @VisibleForTesting
  BeanstalkResource(String configPath) {
    this.configPath = configPath;
  }

  @Override
  public Attributes getAttributes() {
    File configFile = new File(configPath);
    if (!configFile.exists()) {
      return Attributes.empty();
    }

    Attributes.Builder attrBuilders = Attributes.builder();
    try (JsonParser parser = JSON_FACTORY.createParser(configFile)) {
      parser.nextToken();

      if (!parser.isExpectedStartObjectToken()) {
        logger.log(Level.WARNING, "Invalid Beanstalk config: ", configPath);
        return attrBuilders.build();
      }

      while (parser.nextToken() != JsonToken.END_OBJECT) {
        parser.nextValue();
        String value = parser.getText();
        switch (parser.getCurrentName()) {
          case DEVELOPMENT_ID:
            attrBuilders.setAttribute(ResourceAttributes.SERVICE_INSTANCE, value);
            break;
          case VERSION_LABEL:
            attrBuilders.setAttribute(ResourceAttributes.SERVICE_VERSION, value);
            break;
          case ENVIRONMENT_NAME:
            attrBuilders.setAttribute(ResourceAttributes.SERVICE_NAMESPACE, value);
            break;
          default:
            parser.skipChildren();
        }
      }
    } catch (IOException e) {
      logger.log(Level.WARNING, "Could not parse Beanstalk config.", e);
      return Attributes.empty();
    }

    attrBuilders.setAttribute(
        ResourceAttributes.CLOUD_PROVIDER, AwsResourceConstants.cloudProvider());

    return attrBuilders.build();
  }
}
