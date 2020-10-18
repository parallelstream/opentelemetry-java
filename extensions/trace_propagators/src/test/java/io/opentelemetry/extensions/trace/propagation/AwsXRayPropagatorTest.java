/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.extensions.trace.propagation;

import static io.opentelemetry.extensions.trace.propagation.AwsXRayPropagator.TRACE_HEADER_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.trace.Span;
import io.opentelemetry.trace.SpanContext;
import io.opentelemetry.trace.TraceFlags;
import io.opentelemetry.trace.TraceState;
import io.opentelemetry.trace.TracingContextUtils;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.junit.jupiter.api.Test;

class AwsXRayPropagatorTest {

  private static final String TRACE_ID_BASE16 = "8a3c60f7d188f8fa79d48a391a778fa6";
  private static final TraceState TRACE_STATE_DEFAULT = TraceState.getDefault();

  private static final String SPAN_ID_BASE16 = "53995c3f42cd8ad8";
  private static final byte SAMPLED_TRACE_FLAG = TraceFlags.getSampled();

  private static final TextMapPropagator.Setter<Map<String, String>> setter = Map::put;
  private static final TextMapPropagator.Getter<Map<String, String>> getter =
      new TextMapPropagator.Getter<Map<String, String>>() {
        @Nullable
        @Override
        public String get(Map<String, String> carrier, String key) {
          return carrier.get(key);
        }
      };
  private final AwsXRayPropagator xrayPropagator = AwsXRayPropagator.getInstance();

  @Test
  void inject_SampledContext() {
    Map<String, String> carrier = new LinkedHashMap<>();
    xrayPropagator.inject(
        withSpanContext(
            SpanContext.create(
                TRACE_ID_BASE16, SPAN_ID_BASE16, SAMPLED_TRACE_FLAG, TRACE_STATE_DEFAULT),
            Context.current()),
        carrier,
        setter);

    assertThat(carrier)
        .containsEntry(
            TRACE_HEADER_KEY,
            "Root=1-8a3c60f7-d188f8fa79d48a391a778fa6;Parent=53995c3f42cd8ad8;Sampled=1");
  }

  @Test
  void inject_NotSampledContext() {
    Map<String, String> carrier = new LinkedHashMap<>();
    xrayPropagator.inject(
        withSpanContext(
            SpanContext.create(
                TRACE_ID_BASE16, SPAN_ID_BASE16, TraceFlags.getDefault(), TRACE_STATE_DEFAULT),
            Context.current()),
        carrier,
        setter);

    assertThat(carrier)
        .containsEntry(
            TRACE_HEADER_KEY,
            "Root=1-8a3c60f7-d188f8fa79d48a391a778fa6;Parent=53995c3f42cd8ad8;Sampled=0");
  }

  @Test
  void inject_WithTraceState() {
    Map<String, String> carrier = new LinkedHashMap<>();
    xrayPropagator.inject(
        withSpanContext(
            SpanContext.create(
                TRACE_ID_BASE16,
                SPAN_ID_BASE16,
                TraceFlags.getDefault(),
                TraceState.builder().set("foo", "bar").build()),
            Context.current()),
        carrier,
        setter);

    // TODO: assert trace state when the propagator supports it
    assertThat(carrier)
        .containsEntry(
            TRACE_HEADER_KEY,
            "Root=1-8a3c60f7-d188f8fa79d48a391a778fa6;Parent=53995c3f42cd8ad8;Sampled=0");
  }

  @Test
  void extract_Nothing() {
    // Context remains untouched.
    assertThat(
            xrayPropagator.extract(
                Context.current(), Collections.<String, String>emptyMap(), Map::get))
        .isSameAs(Context.current());
  }

  @Test
  void extract_SampledContext() {
    Map<String, String> carrier = new LinkedHashMap<>();
    carrier.put(
        TRACE_HEADER_KEY,
        "Root=1-8a3c60f7-d188f8fa79d48a391a778fa6;Parent=53995c3f42cd8ad8;Sampled=1");

    assertThat(getSpanContext(xrayPropagator.extract(Context.current(), carrier, getter)))
        .isEqualTo(
            SpanContext.createFromRemoteParent(
                TRACE_ID_BASE16, SPAN_ID_BASE16, SAMPLED_TRACE_FLAG, TRACE_STATE_DEFAULT));
  }

  @Test
  void extract_NotSampledContext() {
    Map<String, String> carrier = new LinkedHashMap<>();
    carrier.put(
        TRACE_HEADER_KEY,
        "Root=1-8a3c60f7-d188f8fa79d48a391a778fa6;Parent=53995c3f42cd8ad8;Sampled=0");

    assertThat(getSpanContext(xrayPropagator.extract(Context.current(), carrier, getter)))
        .isEqualTo(
            SpanContext.createFromRemoteParent(
                TRACE_ID_BASE16, SPAN_ID_BASE16, TraceFlags.getDefault(), TRACE_STATE_DEFAULT));
  }

  @Test
  void extract_DifferentPartOrder() {
    Map<String, String> carrier = new LinkedHashMap<>();
    carrier.put(
        TRACE_HEADER_KEY,
        "Parent=53995c3f42cd8ad8;Sampled=1;Root=1-8a3c60f7-d188f8fa79d48a391a778fa6");

    assertThat(getSpanContext(xrayPropagator.extract(Context.current(), carrier, getter)))
        .isEqualTo(
            SpanContext.createFromRemoteParent(
                TRACE_ID_BASE16, SPAN_ID_BASE16, SAMPLED_TRACE_FLAG, TRACE_STATE_DEFAULT));
  }

  @Test
  void extract_AdditionalFields() {
    Map<String, String> carrier = new LinkedHashMap<>();
    carrier.put(
        TRACE_HEADER_KEY,
        "Root=1-8a3c60f7-d188f8fa79d48a391a778fa6;Parent=53995c3f42cd8ad8;Sampled=1;Foo=Bar");

    // TODO: assert additional fields when the propagator supports it
    assertThat(getSpanContext(xrayPropagator.extract(Context.current(), carrier, getter)))
        .isEqualTo(
            SpanContext.createFromRemoteParent(
                TRACE_ID_BASE16, SPAN_ID_BASE16, SAMPLED_TRACE_FLAG, TRACE_STATE_DEFAULT));
  }

  @Test
  void extract_EmptyHeaderValue() {
    Map<String, String> invalidHeaders = new LinkedHashMap<>();
    invalidHeaders.put(TRACE_HEADER_KEY, "");

    assertThat(getSpanContext(xrayPropagator.extract(Context.current(), invalidHeaders, getter)))
        .isSameAs(SpanContext.getInvalid());
  }

  @Test
  void extract_InvalidTraceId() {
    Map<String, String> invalidHeaders = new LinkedHashMap<>();
    invalidHeaders.put(
        TRACE_HEADER_KEY,
        "Root=abcdefghijklmnopabcdefghijklmnop;Parent=53995c3f42cd8ad8;Sampled=0");

    assertThat(getSpanContext(xrayPropagator.extract(Context.current(), invalidHeaders, getter)))
        .isSameAs(SpanContext.getInvalid());
  }

  @Test
  void extract_InvalidTraceId_Size() {
    Map<String, String> invalidHeaders = new LinkedHashMap<>();
    invalidHeaders.put(
        TRACE_HEADER_KEY,
        "Root=1-8a3c60f7-d188f8fa79d48a391a778fa600;Parent=53995c3f42cd8ad8;Sampled=0");

    assertThat(getSpanContext(xrayPropagator.extract(Context.current(), invalidHeaders, getter)))
        .isSameAs(SpanContext.getInvalid());
  }

  @Test
  void extract_InvalidSpanId() {
    Map<String, String> invalidHeaders = new LinkedHashMap<>();
    invalidHeaders.put(
        TRACE_HEADER_KEY,
        "Root=1-8a3c60f7-d188f8fa79d48a391a778fa6;Parent=abcdefghijklmnop;Sampled=0");

    assertThat(getSpanContext(xrayPropagator.extract(Context.current(), invalidHeaders, getter)))
        .isSameAs(SpanContext.getInvalid());
  }

  @Test
  void extract_InvalidSpanId_Size() {
    Map<String, String> invalidHeaders = new LinkedHashMap<>();
    invalidHeaders.put(
        TRACE_HEADER_KEY,
        "Root=1-8a3c60f7-d188f8fa79d48a391a778fa6;Parent=53995c3f42cd8ad800;Sampled=0");

    assertThat(getSpanContext(xrayPropagator.extract(Context.current(), invalidHeaders, getter)))
        .isSameAs(SpanContext.getInvalid());
  }

  @Test
  void extract_InvalidFlags() {
    Map<String, String> invalidHeaders = new LinkedHashMap<>();
    invalidHeaders.put(
        TRACE_HEADER_KEY,
        "Root=1-8a3c60f7-d188f8fa79d48a391a778fa6;Parent=53995c3f42cd8ad8;Sampled=");

    assertThat(getSpanContext(xrayPropagator.extract(Context.current(), invalidHeaders, getter)))
        .isSameAs(SpanContext.getInvalid());
  }

  @Test
  void extract_InvalidFlags_Size() {
    Map<String, String> invalidHeaders = new LinkedHashMap<>();
    invalidHeaders.put(
        TRACE_HEADER_KEY,
        "Root=1-8a3c60f7-d188f8fa79d48a391a778fa6;Parent=53995c3f42cd8ad8;Sampled=10220");

    assertThat(getSpanContext(xrayPropagator.extract(Context.current(), invalidHeaders, getter)))
        .isSameAs(SpanContext.getInvalid());
  }

  @Test
  void extract_InvalidFlags_NonNumeric() {
    Map<String, String> invalidHeaders = new LinkedHashMap<>();
    invalidHeaders.put(
        TRACE_HEADER_KEY,
        "Root=1-8a3c60f7-d188f8fa79d48a391a778fa6;Parent=53995c3f42cd8ad8;Sampled=a");

    assertThat(getSpanContext(xrayPropagator.extract(Context.current(), invalidHeaders, getter)))
        .isSameAs(SpanContext.getInvalid());
  }

  private static Context withSpanContext(SpanContext spanContext, Context context) {
    return TracingContextUtils.withSpan(Span.wrap(spanContext), context);
  }

  private static SpanContext getSpanContext(Context context) {
    return TracingContextUtils.getSpan(context).getContext();
  }
}
