/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.extensions.trace.propagation;

import static io.opentelemetry.extensions.trace.propagation.Common.MAX_TRACE_ID_LENGTH;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.trace.Span;
import io.opentelemetry.trace.SpanContext;
import io.opentelemetry.trace.TraceId;
import io.opentelemetry.trace.TracingContextUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/**
 * Implementation of the Lightstep propagation protocol. Context is propagated through 3 headers,
 * ot-tracer-traceid, ot-tracer-span-id, and ot-tracer-sampled. Baggage is not supported in this
 * implementation. IDs are sent as hex strings and sampled is sent as true or false. See <a
 * href=https://github.com/lightstep/lightstep-tracer-java-common/blob/master/common/src/main/java/com/lightstep/tracer/shared/TextMapPropagator.java>Lightstep
 * TextMapPropagator</a>.
 */
@Immutable
public class OtTracerPropagator implements TextMapPropagator {

  static final String TRACE_ID_HEADER = "ot-tracer-traceid";
  static final String SPAN_ID_HEADER = "ot-tracer-spanid";
  static final String SAMPLED_HEADER = "ot-tracer-sampled";
  private static final List<String> FIELDS =
      Collections.unmodifiableList(Arrays.asList(TRACE_ID_HEADER, SPAN_ID_HEADER, SAMPLED_HEADER));

  private static final OtTracerPropagator INSTANCE = new OtTracerPropagator();

  private OtTracerPropagator() {
    // singleton
  }

  public static OtTracerPropagator getInstance() {
    return INSTANCE;
  }

  @Override
  public List<String> fields() {
    return FIELDS;
  }

  @Override
  public <C> void inject(Context context, C carrier, Setter<C> setter) {
    if (context == null || setter == null) {
      return;
    }
    final SpanContext spanContext = TracingContextUtils.getSpan(context).getContext();
    if (!spanContext.isValid()) {
      return;
    }
    setter.set(carrier, TRACE_ID_HEADER, spanContext.getTraceIdAsHexString());
    setter.set(carrier, SPAN_ID_HEADER, spanContext.getSpanIdAsHexString());
    setter.set(carrier, SAMPLED_HEADER, String.valueOf(spanContext.isSampled()));
  }

  @Override
  public <C> Context extract(Context context, @Nullable C carrier, Getter<C> getter) {
    if (context == null || getter == null) {
      // TODO Other propagators throw exceptions here
      return context;
    }
    String incomingTraceId = getter.get(carrier, TRACE_ID_HEADER);
    String traceId =
        incomingTraceId == null
            ? TraceId.getInvalid()
            : StringUtils.padLeft(incomingTraceId, MAX_TRACE_ID_LENGTH);
    String spanId = getter.get(carrier, SPAN_ID_HEADER);
    String sampled = getter.get(carrier, SAMPLED_HEADER);
    SpanContext spanContext = buildSpanContext(traceId, spanId, sampled);
    if (!spanContext.isValid()) {
      return context;
    }
    return TracingContextUtils.withSpan(Span.wrap(spanContext), context);
  }

  static SpanContext buildSpanContext(String traceId, String spanId, String sampled) {
    if (!Common.isTraceIdValid(traceId) || !Common.isSpanIdValid(spanId)) {
      return SpanContext.getInvalid();
    }
    return Common.buildSpanContext(traceId, spanId, sampled);
  }
}
