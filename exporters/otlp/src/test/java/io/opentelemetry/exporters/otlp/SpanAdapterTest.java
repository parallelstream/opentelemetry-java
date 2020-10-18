/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.exporters.otlp;

import static io.opentelemetry.common.AttributeKey.booleanKey;
import static io.opentelemetry.common.AttributeKey.stringKey;
import static io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_CLIENT;
import static io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_CONSUMER;
import static io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_INTERNAL;
import static io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_PRODUCER;
import static io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER;
import static io.opentelemetry.proto.trace.v1.Status.StatusCode.STATUS_CODE_OK;
import static io.opentelemetry.proto.trace.v1.Status.StatusCode.STATUS_CODE_UNKNOWN_ERROR;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import io.opentelemetry.common.Attributes;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.Status;
import io.opentelemetry.sdk.trace.TestSpanData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.data.SpanData.Event;
import io.opentelemetry.sdk.trace.data.SpanData.Link;
import io.opentelemetry.trace.Span.Kind;
import io.opentelemetry.trace.SpanContext;
import io.opentelemetry.trace.SpanId;
import io.opentelemetry.trace.StatusCode;
import io.opentelemetry.trace.TraceFlags;
import io.opentelemetry.trace.TraceId;
import io.opentelemetry.trace.TraceState;
import java.util.Collections;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link SpanAdapter}. */
class SpanAdapterTest {
  private static final byte[] TRACE_ID_BYTES =
      new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4};
  private static final String TRACE_ID = TraceId.bytesToHex(TRACE_ID_BYTES);
  private static final byte[] SPAN_ID_BYTES = new byte[] {0, 0, 0, 0, 4, 3, 2, 1};
  private static final String SPAN_ID = SpanId.bytesToHex(SPAN_ID_BYTES);

  private static final TraceState TRACE_STATE = TraceState.builder().build();
  private static final SpanContext SPAN_CONTEXT =
      SpanContext.create(TRACE_ID, SPAN_ID, TraceFlags.getSampled(), TRACE_STATE);

  @Test
  void toProtoSpan() {
    Span span =
        SpanAdapter.toProtoSpan(
            TestSpanData.builder()
                .setHasEnded(true)
                .setTraceId(TRACE_ID)
                .setSpanId(SPAN_ID)
                .setParentSpanId(SpanId.getInvalid())
                .setName("GET /api/endpoint")
                .setKind(Kind.SERVER)
                .setStartEpochNanos(12345)
                .setEndEpochNanos(12349)
                .setAttributes(Attributes.of(booleanKey("key"), true))
                .setTotalAttributeCount(2)
                .setEvents(
                    Collections.singletonList(Event.create(12347, "my_event", Attributes.empty())))
                .setTotalRecordedEvents(3)
                .setLinks(Collections.singletonList(Link.create(SPAN_CONTEXT)))
                .setTotalRecordedLinks(2)
                .setStatus(SpanData.Status.ok())
                .build());

    assertThat(span.getTraceId().toByteArray()).isEqualTo(TRACE_ID_BYTES);
    assertThat(span.getSpanId().toByteArray()).isEqualTo(SPAN_ID_BYTES);
    assertThat(span.getParentSpanId().toByteArray()).isEqualTo(new byte[] {});
    assertThat(span.getName()).isEqualTo("GET /api/endpoint");
    assertThat(span.getKind()).isEqualTo(SPAN_KIND_SERVER);
    assertThat(span.getStartTimeUnixNano()).isEqualTo(12345);
    assertThat(span.getEndTimeUnixNano()).isEqualTo(12349);
    assertThat(span.getAttributesList())
        .containsExactly(
            KeyValue.newBuilder()
                .setKey("key")
                .setValue(AnyValue.newBuilder().setBoolValue(true).build())
                .build());
    assertThat(span.getDroppedAttributesCount()).isEqualTo(1);
    assertThat(span.getEventsList())
        .containsExactly(
            Span.Event.newBuilder().setTimeUnixNano(12347).setName("my_event").build());
    assertThat(span.getDroppedEventsCount()).isEqualTo(2); // 3 - 1
    assertThat(span.getLinksList())
        .containsExactly(
            Span.Link.newBuilder()
                .setTraceId(ByteString.copyFrom(TRACE_ID_BYTES))
                .setSpanId(ByteString.copyFrom(SPAN_ID_BYTES))
                .build());
    assertThat(span.getDroppedLinksCount()).isEqualTo(1); // 2 - 1
    assertThat(span.getStatus()).isEqualTo(Status.newBuilder().setCode(STATUS_CODE_OK).build());
  }

  @Test
  void toProtoSpanKind() {
    assertThat(SpanAdapter.toProtoSpanKind(Kind.INTERNAL)).isEqualTo(SPAN_KIND_INTERNAL);
    assertThat(SpanAdapter.toProtoSpanKind(Kind.CLIENT)).isEqualTo(SPAN_KIND_CLIENT);
    assertThat(SpanAdapter.toProtoSpanKind(Kind.SERVER)).isEqualTo(SPAN_KIND_SERVER);
    assertThat(SpanAdapter.toProtoSpanKind(Kind.PRODUCER)).isEqualTo(SPAN_KIND_PRODUCER);
    assertThat(SpanAdapter.toProtoSpanKind(Kind.CONSUMER)).isEqualTo(SPAN_KIND_CONSUMER);
  }

  @Test
  void toProtoStatus() {
    assertThat(SpanAdapter.toStatusProto(SpanData.Status.unset()))
        .isEqualTo(Status.newBuilder().setCode(STATUS_CODE_OK).build());
    assertThat(SpanAdapter.toStatusProto(SpanData.Status.create(StatusCode.ERROR, "ERROR")))
        .isEqualTo(
            Status.newBuilder().setCode(STATUS_CODE_UNKNOWN_ERROR).setMessage("ERROR").build());
    assertThat(SpanAdapter.toStatusProto(SpanData.Status.create(StatusCode.ERROR, "UNKNOWN")))
        .isEqualTo(
            Status.newBuilder().setCode(STATUS_CODE_UNKNOWN_ERROR).setMessage("UNKNOWN").build());
    assertThat(SpanAdapter.toStatusProto(SpanData.Status.create(StatusCode.OK, "OK_OVERRIDE")))
        .isEqualTo(Status.newBuilder().setCode(STATUS_CODE_OK).setMessage("OK_OVERRIDE").build());
  }

  @Test
  void toProtoSpanEvent_WithoutAttributes() {
    assertThat(
            SpanAdapter.toProtoSpanEvent(
                Event.create(12345, "test_without_attributes", Attributes.empty())))
        .isEqualTo(
            Span.Event.newBuilder()
                .setTimeUnixNano(12345)
                .setName("test_without_attributes")
                .build());
  }

  @Test
  void toProtoSpanEvent_WithAttributes() {
    assertThat(
            SpanAdapter.toProtoSpanEvent(
                Event.create(
                    12345,
                    "test_with_attributes",
                    Attributes.of(stringKey("key_string"), "string"),
                    5)))
        .isEqualTo(
            Span.Event.newBuilder()
                .setTimeUnixNano(12345)
                .setName("test_with_attributes")
                .addAttributes(
                    KeyValue.newBuilder()
                        .setKey("key_string")
                        .setValue(AnyValue.newBuilder().setStringValue("string").build())
                        .build())
                .setDroppedAttributesCount(4)
                .build());
  }

  @Test
  void toProtoSpanLink_WithoutAttributes() {
    assertThat(SpanAdapter.toProtoSpanLink(Link.create(SPAN_CONTEXT)))
        .isEqualTo(
            Span.Link.newBuilder()
                .setTraceId(ByteString.copyFrom(TRACE_ID_BYTES))
                .setSpanId(ByteString.copyFrom(SPAN_ID_BYTES))
                .build());
  }

  @Test
  void toProtoSpanLink_WithAttributes() {
    assertThat(
            SpanAdapter.toProtoSpanLink(
                Link.create(SPAN_CONTEXT, Attributes.of(stringKey("key_string"), "string"), 5)))
        .isEqualTo(
            Span.Link.newBuilder()
                .setTraceId(ByteString.copyFrom(TRACE_ID_BYTES))
                .setSpanId(ByteString.copyFrom(SPAN_ID_BYTES))
                .addAttributes(
                    KeyValue.newBuilder()
                        .setKey("key_string")
                        .setValue(AnyValue.newBuilder().setStringValue("string").build())
                        .build())
                .setDroppedAttributesCount(4)
                .build());
  }
}
