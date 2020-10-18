/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.trace.propagation;

import static io.opentelemetry.internal.Utils.checkArgument;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.internal.TemporaryBuffers;
import io.opentelemetry.trace.Span;
import io.opentelemetry.trace.SpanContext;
import io.opentelemetry.trace.SpanId;
import io.opentelemetry.trace.TraceFlags;
import io.opentelemetry.trace.TraceId;
import io.opentelemetry.trace.TraceState;
import io.opentelemetry.trace.TracingContextUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import javax.annotation.concurrent.Immutable;

/**
 * Implementation of the TraceContext propagation protocol. See <a
 * href=https://github.com/w3c/distributed-tracing>w3c/distributed-tracing</a>.
 */
@Immutable
public class HttpTraceContext implements TextMapPropagator {
  private static final Logger logger = Logger.getLogger(HttpTraceContext.class.getName());

  private static final TraceState TRACE_STATE_DEFAULT = TraceState.builder().build();
  static final String TRACE_PARENT = "traceparent";
  static final String TRACE_STATE = "tracestate";
  private static final List<String> FIELDS =
      Collections.unmodifiableList(Arrays.asList(TRACE_PARENT, TRACE_STATE));

  private static final String VERSION = "00";
  private static final int VERSION_SIZE = 2;
  private static final char TRACEPARENT_DELIMITER = '-';
  private static final int TRACEPARENT_DELIMITER_SIZE = 1;
  private static final int TRACE_ID_HEX_SIZE = TraceId.getHexLength();
  private static final int SPAN_ID_HEX_SIZE = SpanId.getHexLength();
  private static final int TRACE_OPTION_HEX_SIZE = TraceFlags.getHexLength();
  private static final int TRACE_ID_OFFSET = VERSION_SIZE + TRACEPARENT_DELIMITER_SIZE;
  private static final int SPAN_ID_OFFSET =
      TRACE_ID_OFFSET + TRACE_ID_HEX_SIZE + TRACEPARENT_DELIMITER_SIZE;
  private static final int TRACE_OPTION_OFFSET =
      SPAN_ID_OFFSET + SPAN_ID_HEX_SIZE + TRACEPARENT_DELIMITER_SIZE;
  private static final int TRACEPARENT_HEADER_SIZE = TRACE_OPTION_OFFSET + TRACE_OPTION_HEX_SIZE;
  private static final int TRACESTATE_MAX_SIZE = 512;
  private static final int TRACESTATE_MAX_MEMBERS = 32;
  private static final char TRACESTATE_KEY_VALUE_DELIMITER = '=';
  private static final char TRACESTATE_ENTRY_DELIMITER = ',';
  private static final Pattern TRACESTATE_ENTRY_DELIMITER_SPLIT_PATTERN =
      Pattern.compile("[ \t]*" + TRACESTATE_ENTRY_DELIMITER + "[ \t]*");
  private static final Set<String> VALID_VERSIONS;
  private static final String VERSION_00 = "00";
  private static final HttpTraceContext INSTANCE = new HttpTraceContext();

  static {
    // A valid version is 1 byte representing an 8-bit unsigned integer, version ff is invalid.
    VALID_VERSIONS = new HashSet<>();
    for (int i = 0; i < 255; i++) {
      String version = Long.toHexString(i);
      if (version.length() < 2) {
        version = '0' + version;
      }
      VALID_VERSIONS.add(version);
    }
  }

  private HttpTraceContext() {
    // singleton
  }

  public static HttpTraceContext getInstance() {
    return INSTANCE;
  }

  @Override
  public List<String> fields() {
    return FIELDS;
  }

  @Override
  public <C> void inject(Context context, C carrier, Setter<C> setter) {
    Objects.requireNonNull(context, "context");
    Objects.requireNonNull(setter, "setter");

    SpanContext spanContext = TracingContextUtils.getSpan(context).getContext();
    if (!spanContext.isValid()) {
      return;
    }

    char[] chars = TemporaryBuffers.chars(TRACEPARENT_HEADER_SIZE);
    chars[0] = VERSION.charAt(0);
    chars[1] = VERSION.charAt(1);
    chars[2] = TRACEPARENT_DELIMITER;

    String traceId = spanContext.getTraceIdAsHexString();
    for (int i = 0; i < traceId.length(); i++) {
      chars[TRACE_ID_OFFSET + i] = traceId.charAt(i);
    }

    chars[SPAN_ID_OFFSET - 1] = TRACEPARENT_DELIMITER;

    String spanId = spanContext.getSpanIdAsHexString();
    for (int i = 0; i < spanId.length(); i++) {
      chars[SPAN_ID_OFFSET + i] = spanId.charAt(i);
    }

    chars[TRACE_OPTION_OFFSET - 1] = TRACEPARENT_DELIMITER;
    spanContext.copyTraceFlagsHexTo(chars, TRACE_OPTION_OFFSET);
    setter.set(carrier, TRACE_PARENT, new String(chars, 0, TRACEPARENT_HEADER_SIZE));
    List<TraceState.Entry> entries = spanContext.getTraceState().getEntries();
    if (entries.isEmpty()) {
      // No need to add an empty "tracestate" header.
      return;
    }
    StringBuilder stringBuilder = new StringBuilder(TRACESTATE_MAX_SIZE);
    for (TraceState.Entry entry : entries) {
      if (stringBuilder.length() != 0) {
        stringBuilder.append(TRACESTATE_ENTRY_DELIMITER);
      }
      stringBuilder
          .append(entry.getKey())
          .append(TRACESTATE_KEY_VALUE_DELIMITER)
          .append(entry.getValue());
    }
    setter.set(carrier, TRACE_STATE, stringBuilder.toString());
  }

  @Override
  public <C /*>>> extends @NonNull Object*/> Context extract(
      Context context, C carrier, Getter<C> getter) {
    Objects.requireNonNull(context, "context");
    Objects.requireNonNull(carrier, "carrier");
    Objects.requireNonNull(getter, "getter");

    SpanContext spanContext = extractImpl(carrier, getter);
    if (!spanContext.isValid()) {
      return context;
    }

    return TracingContextUtils.withSpan(Span.wrap(spanContext), context);
  }

  private static <C> SpanContext extractImpl(C carrier, Getter<C> getter) {
    String traceParent = getter.get(carrier, TRACE_PARENT);
    if (traceParent == null) {
      return SpanContext.getInvalid();
    }

    SpanContext contextFromParentHeader = extractContextFromTraceParent(traceParent);
    if (!contextFromParentHeader.isValid()) {
      return contextFromParentHeader;
    }

    String traceStateHeader = getter.get(carrier, TRACE_STATE);
    if (traceStateHeader == null || traceStateHeader.isEmpty()) {
      return contextFromParentHeader;
    }

    try {
      TraceState traceState = extractTraceState(traceStateHeader);
      return SpanContext.createFromRemoteParent(
          contextFromParentHeader.getTraceIdAsHexString(),
          contextFromParentHeader.getSpanIdAsHexString(),
          contextFromParentHeader.getTraceFlags(),
          traceState);
    } catch (IllegalArgumentException e) {
      logger.info("Unparseable tracestate header. Returning span context without state.");
      return contextFromParentHeader;
    }
  }

  private static SpanContext extractContextFromTraceParent(String traceparent) {
    // TODO(bdrutu): Do we need to verify that version is hex and that
    // for the version the length is the expected one?
    boolean isValid =
        (traceparent.length() == TRACEPARENT_HEADER_SIZE
                || (traceparent.length() > TRACEPARENT_HEADER_SIZE
                    && traceparent.charAt(TRACEPARENT_HEADER_SIZE) == TRACEPARENT_DELIMITER))
            && traceparent.charAt(TRACE_ID_OFFSET - 1) == TRACEPARENT_DELIMITER
            && traceparent.charAt(SPAN_ID_OFFSET - 1) == TRACEPARENT_DELIMITER
            && traceparent.charAt(TRACE_OPTION_OFFSET - 1) == TRACEPARENT_DELIMITER;
    if (!isValid) {
      logger.info("Unparseable traceparent header. Returning INVALID span context.");
      return SpanContext.getInvalid();
    }

    try {
      String version = traceparent.substring(0, 2);
      if (!VALID_VERSIONS.contains(version)) {
        return SpanContext.getInvalid();
      }
      if (version.equals(VERSION_00) && traceparent.length() > TRACEPARENT_HEADER_SIZE) {
        return SpanContext.getInvalid();
      }

      String traceId =
          traceparent.substring(TRACE_ID_OFFSET, TRACE_ID_OFFSET + TraceId.getHexLength());
      String spanId = traceparent.substring(SPAN_ID_OFFSET, SPAN_ID_OFFSET + SpanId.getHexLength());
      if (TraceId.isValid(traceId) && SpanId.isValid(spanId)) {
        byte isSampled = TraceFlags.byteFromHex(traceparent, TRACE_OPTION_OFFSET);
        return SpanContext.createFromRemoteParent(traceId, spanId, isSampled, TRACE_STATE_DEFAULT);
      }
      return SpanContext.getInvalid();
    } catch (IllegalArgumentException e) {
      logger.info("Unparseable traceparent header. Returning INVALID span context.");
      return SpanContext.getInvalid();
    }
  }

  private static TraceState extractTraceState(String traceStateHeader) {
    TraceState.Builder traceStateBuilder = TraceState.builder();
    String[] listMembers = TRACESTATE_ENTRY_DELIMITER_SPLIT_PATTERN.split(traceStateHeader);
    checkArgument(
        listMembers.length <= TRACESTATE_MAX_MEMBERS, "TraceState has too many elements.");
    // Iterate in reverse order because when call builder set the elements is added in the
    // front of the list.
    for (int i = listMembers.length - 1; i >= 0; i--) {
      String listMember = listMembers[i];
      int index = listMember.indexOf(TRACESTATE_KEY_VALUE_DELIMITER);
      checkArgument(index != -1, "Invalid TraceState list-member format.");
      traceStateBuilder.set(listMember.substring(0, index), listMember.substring(index + 1));
    }
    return traceStateBuilder.build();
  }
}
