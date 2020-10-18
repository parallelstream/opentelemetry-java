/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.extensions.trace.propagation;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.trace.Span;
import io.opentelemetry.trace.SpanContext;
import io.opentelemetry.trace.SpanId;
import io.opentelemetry.trace.TraceFlags;
import io.opentelemetry.trace.TraceId;
import io.opentelemetry.trace.TraceState;
import io.opentelemetry.trace.TracingContextUtils;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * Implementation of the AWS X-Ray Trace Header propagation protocol. See <a href=
 * https://https://docs.aws.amazon.com/xray/latest/devguide/xray-concepts.html#xray-concepts-tracingheader>AWS
 * Tracing header spec</a>
 *
 * <p>To register the X-Ray propagator together with default propagator:
 *
 * <pre>{@code
 * OpenTelemetry.setPropagators(
 *   DefaultContextPropagators
 *     .builder()
 *     .addTextMapPropagator(new HttpTraceContext())
 *     .addTextMapPropagator(new AWSXRayPropagator())
 *     .build());
 * }</pre>
 */
public class AwsXRayPropagator implements TextMapPropagator {

  // Visible for testing
  static final String TRACE_HEADER_KEY = "X-Amzn-Trace-Id";

  private static final Logger logger = Logger.getLogger(AwsXRayPropagator.class.getName());

  private static final char TRACE_HEADER_DELIMITER = ';';
  private static final char KV_DELIMITER = '=';

  private static final String TRACE_ID_KEY = "Root";
  private static final int TRACE_ID_LENGTH = 35;
  private static final String TRACE_ID_VERSION = "1";
  private static final char TRACE_ID_DELIMITER = '-';
  private static final int TRACE_ID_DELIMITER_INDEX_1 = 1;
  private static final int TRACE_ID_DELIMITER_INDEX_2 = 10;
  private static final int TRACE_ID_FIRST_PART_LENGTH = 8;

  private static final String PARENT_ID_KEY = "Parent";
  private static final int PARENT_ID_LENGTH = 16;

  private static final String SAMPLED_FLAG_KEY = "Sampled";
  private static final int SAMPLED_FLAG_LENGTH = 1;
  private static final char IS_SAMPLED = '1';
  private static final char NOT_SAMPLED = '0';

  private static final List<String> FIELDS = Collections.singletonList(TRACE_HEADER_KEY);

  private static final AwsXRayPropagator INSTANCE = new AwsXRayPropagator();

  private AwsXRayPropagator() {
    // singleton
  }

  public static AwsXRayPropagator getInstance() {
    return INSTANCE;
  }

  @Override
  public List<String> fields() {
    return FIELDS;
  }

  @Override
  public <C> void inject(Context context, @Nullable C carrier, Setter<C> setter) {
    Objects.requireNonNull(context, "context");
    Objects.requireNonNull(setter, "setter");

    Span span = TracingContextUtils.getSpan(context);
    if (!span.getContext().isValid()) {
      return;
    }

    SpanContext spanContext = span.getContext();

    String otTraceId = spanContext.getTraceIdAsHexString();
    String xrayTraceId =
        TRACE_ID_VERSION
            + TRACE_ID_DELIMITER
            + otTraceId.substring(0, TRACE_ID_FIRST_PART_LENGTH)
            + TRACE_ID_DELIMITER
            + otTraceId.substring(TRACE_ID_FIRST_PART_LENGTH);
    String parentId = spanContext.getSpanIdAsHexString();
    char samplingFlag = spanContext.isSampled() ? IS_SAMPLED : NOT_SAMPLED;
    // TODO: Add OT trace state to the X-Ray trace header

    String traceHeader =
        TRACE_ID_KEY
            + KV_DELIMITER
            + xrayTraceId
            + TRACE_HEADER_DELIMITER
            + PARENT_ID_KEY
            + KV_DELIMITER
            + parentId
            + TRACE_HEADER_DELIMITER
            + SAMPLED_FLAG_KEY
            + KV_DELIMITER
            + samplingFlag;
    setter.set(carrier, TRACE_HEADER_KEY, traceHeader);
  }

  @Override
  public <C> Context extract(Context context, C carrier, Getter<C> getter) {
    Objects.requireNonNull(carrier, "carrier");
    Objects.requireNonNull(getter, "getter");

    SpanContext spanContext = getSpanContextFromHeader(carrier, getter);
    if (!spanContext.isValid()) {
      return context;
    }

    return TracingContextUtils.withSpan(Span.wrap(spanContext), context);
  }

  private static <C> SpanContext getSpanContextFromHeader(C carrier, Getter<C> getter) {
    String traceHeader = getter.get(carrier, TRACE_HEADER_KEY);
    if (traceHeader == null || traceHeader.isEmpty()) {
      return SpanContext.getInvalid();
    }

    String traceId = TraceId.getInvalid();
    String spanId = SpanId.getInvalid();
    Boolean isSampled = false;

    int pos = 0;
    while (pos < traceHeader.length()) {
      int delimiterIndex = traceHeader.indexOf(TRACE_HEADER_DELIMITER, pos);
      final String part;
      if (delimiterIndex >= 0) {
        part = traceHeader.substring(pos, delimiterIndex);
        pos = delimiterIndex + 1;
      } else {
        // Last part.
        part = traceHeader.substring(pos);
        pos = traceHeader.length();
      }
      String trimmedPart = part.trim();
      int equalsIndex = trimmedPart.indexOf(KV_DELIMITER);
      if (equalsIndex < 0) {
        logger.fine(
            "Error parsing X-Ray trace header. Invalid key value pair: "
                + part
                + " Returning INVALID span context.");
        return SpanContext.getInvalid();
      }

      String value = trimmedPart.substring(equalsIndex + 1);

      if (trimmedPart.startsWith(TRACE_ID_KEY)) {
        traceId = parseTraceId(value);
      } else if (trimmedPart.startsWith(PARENT_ID_KEY)) {
        spanId = parseSpanId(value);
      } else if (trimmedPart.startsWith(SAMPLED_FLAG_KEY)) {
        isSampled = parseTraceFlag(value);
      }
      // TODO: Put the arbitrary TraceHeader keys in OT trace state
    }
    if (!TraceId.isValid(traceId)) {
      logger.fine(
          "Invalid TraceId in X-Ray trace header: '"
              + TRACE_HEADER_KEY
              + "' with value "
              + traceHeader
              + "'. Returning INVALID span context.");
      return SpanContext.getInvalid();
    }

    if (!SpanId.isValid(spanId)) {
      logger.fine(
          "Invalid ParentId in X-Ray trace header: '"
              + TRACE_HEADER_KEY
              + "' with value "
              + traceHeader
              + "'. Returning INVALID span context.");
      return SpanContext.getInvalid();
    }

    if (isSampled == null) {
      logger.fine(
          "Invalid Sampling flag in X-Ray trace header: '"
              + TRACE_HEADER_KEY
              + "' with value "
              + traceHeader
              + "'. Returning INVALID span context.");
      return SpanContext.getInvalid();
    }

    byte traceFlags = isSampled ? TraceFlags.getSampled() : TraceFlags.getDefault();
    return SpanContext.createFromRemoteParent(traceId, spanId, traceFlags, TraceState.getDefault());
  }

  private static String parseTraceId(String xrayTraceId) {
    if (xrayTraceId.length() != TRACE_ID_LENGTH) {
      return TraceId.getInvalid();
    }

    // Check version trace id version
    if (!xrayTraceId.startsWith(TRACE_ID_VERSION)) {
      return TraceId.getInvalid();
    }

    // Check delimiters
    if (xrayTraceId.charAt(TRACE_ID_DELIMITER_INDEX_1) != TRACE_ID_DELIMITER
        || xrayTraceId.charAt(TRACE_ID_DELIMITER_INDEX_2) != TRACE_ID_DELIMITER) {
      return TraceId.getInvalid();
    }

    String epochPart =
        xrayTraceId.substring(TRACE_ID_DELIMITER_INDEX_1 + 1, TRACE_ID_DELIMITER_INDEX_2);
    String uniquePart = xrayTraceId.substring(TRACE_ID_DELIMITER_INDEX_2 + 1, TRACE_ID_LENGTH);

    // X-Ray trace id format is 1-{8 digit hex}-{24 digit hex}
    return epochPart + uniquePart;
  }

  private static String parseSpanId(String xrayParentId) {
    if (xrayParentId.length() != PARENT_ID_LENGTH) {
      return SpanId.getInvalid();
    }

    return xrayParentId;
  }

  @Nullable
  private static Boolean parseTraceFlag(String xraySampledFlag) {
    if (xraySampledFlag.length() != SAMPLED_FLAG_LENGTH) {
      // Returning null as there is no invalid trace flag defined.
      return null;
    }

    char flag = xraySampledFlag.charAt(0);
    if (flag == IS_SAMPLED) {
      return true;
    } else if (flag == NOT_SAMPLED) {
      return false;
    } else {
      return null;
    }
  }
}
