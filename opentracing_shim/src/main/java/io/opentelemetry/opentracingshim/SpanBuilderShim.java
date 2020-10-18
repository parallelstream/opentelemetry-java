/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.opentracingshim;

import static io.opentelemetry.common.AttributeKey.booleanKey;
import static io.opentelemetry.common.AttributeKey.doubleKey;
import static io.opentelemetry.common.AttributeKey.longKey;
import static io.opentelemetry.common.AttributeKey.stringKey;

import io.opentelemetry.baggage.Baggage;
import io.opentelemetry.common.AttributeKey;
import io.opentelemetry.context.Context;
import io.opentelemetry.trace.Span.Kind;
import io.opentelemetry.trace.StatusCode;
import io.opentelemetry.trace.TracingContextUtils;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer.SpanBuilder;
import io.opentracing.tag.Tag;
import io.opentracing.tag.Tags;
import java.util.ArrayList;
import java.util.List;

final class SpanBuilderShim extends BaseShimObject implements SpanBuilder {
  private final String spanName;

  // The parent will be either a Span or a SpanContext.
  // Inherited baggage is supported only for the main parent.
  private SpanShim parentSpan;
  private SpanContextShim parentSpanContext;
  private boolean ignoreActiveSpan;

  private final List<io.opentelemetry.trace.SpanContext> parentLinks = new ArrayList<>();

  @SuppressWarnings("rawtypes")
  private final List<AttributeKey> spanBuilderAttributeKeys = new ArrayList<>();

  private final List<Object> spanBuilderAttributeValues = new ArrayList<>();
  private Kind spanKind;
  private boolean error;

  public SpanBuilderShim(TelemetryInfo telemetryInfo, String spanName) {
    super(telemetryInfo);
    this.spanName = spanName;
  }

  @Override
  public SpanBuilder asChildOf(Span parent) {
    if (parent == null) {
      return this;
    }

    // TODO - Verify we handle a no-op Span
    SpanShim spanShim = getSpanShim(parent);

    if (parentSpan == null && parentSpanContext == null) {
      parentSpan = spanShim;
    } else {
      parentLinks.add(spanShim.getSpan().getContext());
    }

    return this;
  }

  @Override
  public SpanBuilder asChildOf(SpanContext parent) {
    return addReference(null, parent);
  }

  @Override
  public SpanBuilder addReference(String referenceType, SpanContext referencedContext) {
    if (referencedContext == null) {
      return this;
    }

    // TODO - Use referenceType
    SpanContextShim contextShim = getContextShim(referencedContext);

    if (parentSpan == null && parentSpanContext == null) {
      parentSpanContext = contextShim;
    } else {
      parentLinks.add(contextShim.getSpanContext());
    }

    return this;
  }

  @Override
  public SpanBuilder ignoreActiveSpan() {
    ignoreActiveSpan = true;
    return this;
  }

  @Override
  public SpanBuilder withTag(String key, String value) {
    if (Tags.SPAN_KIND.getKey().equals(key)) {
      switch (value) {
        case Tags.SPAN_KIND_CLIENT:
          spanKind = Kind.CLIENT;
          break;
        case Tags.SPAN_KIND_SERVER:
          spanKind = Kind.SERVER;
          break;
        case Tags.SPAN_KIND_PRODUCER:
          spanKind = Kind.PRODUCER;
          break;
        case Tags.SPAN_KIND_CONSUMER:
          spanKind = Kind.CONSUMER;
          break;
        default:
          spanKind = Kind.INTERNAL;
          break;
      }
    } else if (Tags.ERROR.getKey().equals(key)) {
      error = Boolean.parseBoolean(value);
    } else {
      this.spanBuilderAttributeKeys.add(stringKey(key));
      this.spanBuilderAttributeValues.add(value);
    }

    return this;
  }

  @Override
  public SpanBuilder withTag(String key, boolean value) {
    if (Tags.ERROR.getKey().equals(key)) {
      error = value;
    } else {
      this.spanBuilderAttributeKeys.add(booleanKey(key));
      this.spanBuilderAttributeValues.add(value);
    }
    return this;
  }

  @Override
  public SpanBuilder withTag(String key, Number value) {
    // TODO - Verify only the 'basic' types are supported/used.
    if (value instanceof Integer
        || value instanceof Long
        || value instanceof Short
        || value instanceof Byte) {
      this.spanBuilderAttributeKeys.add(longKey(key));
      this.spanBuilderAttributeValues.add(value.longValue());
    } else if (value instanceof Float || value instanceof Double) {
      this.spanBuilderAttributeKeys.add(doubleKey(key));
      this.spanBuilderAttributeValues.add(value.doubleValue());
    } else {
      throw new IllegalArgumentException("Number type not supported");
    }

    return this;
  }

  @Override
  public <T> SpanBuilder withTag(Tag<T> tag, T value) {
    if (value instanceof String) {
      this.withTag(tag.getKey(), (String) value);
    } else if (value instanceof Boolean) {
      this.withTag(tag.getKey(), (Boolean) value);
    } else if (value instanceof Number) {
      this.withTag(tag.getKey(), (Number) value);
    } else {
      this.withTag(tag.getKey(), value.toString());
    }

    return this;
  }

  @Override
  public SpanBuilder withStartTimestamp(long microseconds) {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public Span start() {
    Baggage baggage = null;
    io.opentelemetry.trace.Span.Builder builder = tracer().spanBuilder(spanName);

    if (ignoreActiveSpan && parentSpan == null && parentSpanContext == null) {
      builder.setNoParent();
    } else if (parentSpan != null) {
      builder.setParent(TracingContextUtils.withSpan(parentSpan.getSpan(), Context.root()));
      SpanContextShim contextShim = spanContextTable().get(parentSpan);
      baggage = contextShim == null ? null : contextShim.getBaggage();
    } else if (parentSpanContext != null) {
      builder.setParent(
          TracingContextUtils.withSpan(
              io.opentelemetry.trace.Span.wrap(parentSpanContext.getSpanContext()),
              Context.root()));
      baggage = parentSpanContext.getBaggage();
    }

    for (io.opentelemetry.trace.SpanContext link : parentLinks) {
      builder.addLink(link);
    }

    if (spanKind != null) {
      builder.setSpanKind(spanKind);
    }

    io.opentelemetry.trace.Span span = builder.startSpan();

    for (int i = 0; i < this.spanBuilderAttributeKeys.size(); i++) {
      AttributeKey key = this.spanBuilderAttributeKeys.get(i);
      Object value = this.spanBuilderAttributeValues.get(i);
      span.setAttribute(key, value);
    }
    if (error) {
      span.setStatus(StatusCode.ERROR);
    }

    SpanShim spanShim = new SpanShim(telemetryInfo(), span);

    if (baggage != null && baggage != telemetryInfo().emptyBaggage()) {
      spanContextTable().create(spanShim, baggage);
    }

    return spanShim;
  }

  private static SpanShim getSpanShim(Span span) {
    if (!(span instanceof SpanShim)) {
      throw new IllegalArgumentException("span is not a valid SpanShim object");
    }

    return (SpanShim) span;
  }

  private static SpanContextShim getContextShim(SpanContext context) {
    if (!(context instanceof SpanContextShim)) {
      throw new IllegalArgumentException("context is not a valid SpanContextShim object");
    }

    return (SpanContextShim) context;
  }
}
