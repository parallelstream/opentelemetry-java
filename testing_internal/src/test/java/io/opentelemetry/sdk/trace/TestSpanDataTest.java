/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.trace;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.opentelemetry.common.Attributes;
import io.opentelemetry.sdk.common.InstrumentationLibraryInfo;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.data.SpanData.Event;
import io.opentelemetry.sdk.trace.data.SpanData.Link;
import io.opentelemetry.sdk.trace.data.SpanData.Status;
import io.opentelemetry.trace.Span.Kind;
import io.opentelemetry.trace.SpanContext;
import io.opentelemetry.trace.SpanId;
import io.opentelemetry.trace.TraceId;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class TestSpanDataTest {

  private static final long START_EPOCH_NANOS = TimeUnit.SECONDS.toNanos(3000) + 200;
  private static final long END_EPOCH_NANOS = TimeUnit.SECONDS.toNanos(3001) + 255;

  @Test
  void defaultValues() {
    SpanData spanData = createBasicSpanBuilder().build();

    assertThat(SpanId.isValid(spanData.getParentSpanId())).isFalse();
    assertThat(spanData.getAttributes()).isEqualTo(Attributes.empty());
    assertThat(spanData.getEvents()).isEqualTo(emptyList());
    assertThat(spanData.getLinks()).isEqualTo(emptyList());
    assertThat(spanData.getInstrumentationLibraryInfo())
        .isSameAs(InstrumentationLibraryInfo.getEmpty());
    assertThat(spanData.getHasRemoteParent()).isFalse();
  }

  @Test
  void unmodifiableLinks() {
    SpanData spanData = createSpanDataWithMutableCollections();

    assertThrows(UnsupportedOperationException.class, () -> spanData.getLinks().add(emptyLink()));
  }

  @Test
  void unmodifiableTimedEvents() {
    SpanData spanData = createSpanDataWithMutableCollections();

    assertThrows(
        UnsupportedOperationException.class,
        () -> spanData.getEvents().add(Event.create(1234, "foo", Attributes.empty())));
  }

  @Test
  void defaultTotalAttributeCountIsZero() {
    SpanData spanData = createSpanDataWithMutableCollections();
    assertThat(spanData.getTotalAttributeCount()).isEqualTo(0);
  }

  @Test
  void canSetTotalAttributeCountWithBuilder() {
    SpanData spanData = createBasicSpanBuilder().setTotalAttributeCount(123).build();
    assertThat(spanData.getTotalAttributeCount()).isEqualTo(123);
  }

  @Test
  void link_defaultTotalAttributeCountIsZero() {
    Link link = Link.create(SpanContext.getInvalid());
    assertThat(link.getTotalAttributeCount()).isEqualTo(0);
  }

  @Test
  void link_canSetTotalAttributeCount() {
    Link link = Link.create(SpanContext.getInvalid());
    assertThat(link.getTotalAttributeCount()).isEqualTo(0);
  }

  @Test
  void timedEvent_defaultTotalAttributeCountIsZero() {
    Event event = Event.create(START_EPOCH_NANOS, "foo", Attributes.empty());
    assertThat(event.getTotalAttributeCount()).isEqualTo(0);
  }

  @Test
  void timedEvent_canSetTotalAttributeCount() {
    Event event = Event.create(START_EPOCH_NANOS, "foo", Attributes.empty(), 123);
    assertThat(event.getTotalAttributeCount()).isEqualTo(123);
  }

  private static SpanData createSpanDataWithMutableCollections() {
    return createBasicSpanBuilder()
        .setLinks(new ArrayList<>())
        .setEvents(new ArrayList<>())
        .build();
  }

  private static Link emptyLink() {
    return Link.create(SpanContext.getInvalid());
  }

  private static TestSpanData.Builder createBasicSpanBuilder() {
    return TestSpanData.builder()
        .setHasEnded(true)
        .setSpanId(SpanId.getInvalid())
        .setTraceId(TraceId.getInvalid())
        .setName("spanName")
        .setStartEpochNanos(START_EPOCH_NANOS)
        .setEndEpochNanos(END_EPOCH_NANOS)
        .setKind(Kind.SERVER)
        .setStatus(Status.ok())
        .setHasRemoteParent(false)
        .setTotalRecordedEvents(0)
        .setTotalRecordedLinks(0);
  }
}
