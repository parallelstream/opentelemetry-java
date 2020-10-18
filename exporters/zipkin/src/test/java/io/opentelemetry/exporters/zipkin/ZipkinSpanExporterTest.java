/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.exporters.zipkin;

import static io.opentelemetry.common.AttributeKey.booleanArrayKey;
import static io.opentelemetry.common.AttributeKey.booleanKey;
import static io.opentelemetry.common.AttributeKey.doubleArrayKey;
import static io.opentelemetry.common.AttributeKey.doubleKey;
import static io.opentelemetry.common.AttributeKey.longArrayKey;
import static io.opentelemetry.common.AttributeKey.longKey;
import static io.opentelemetry.common.AttributeKey.stringArrayKey;
import static io.opentelemetry.common.AttributeKey.stringKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.opentelemetry.common.Attributes;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.InstrumentationLibraryInfo;
import io.opentelemetry.sdk.common.export.ConfigBuilder;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.resources.ResourceAttributes;
import io.opentelemetry.sdk.trace.TestSpanData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.data.SpanData.Event;
import io.opentelemetry.trace.Span.Kind;
import io.opentelemetry.trace.StatusCode;
import io.opentelemetry.trace.attributes.SemanticAttributes;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.reporter.Sender;

@ExtendWith(MockitoExtension.class)
class ZipkinSpanExporterTest {

  @Mock private Sender mockSender;
  @Mock private SpanBytesEncoder mockEncoder;
  @Mock private Call<Void> mockZipkinCall;

  private static final Endpoint localEndpoint =
      ZipkinSpanExporter.produceLocalEndpoint("tweetiebird");
  private static final String TRACE_ID = "d239036e7d5cec116b562147388b35bf";
  private static final String SPAN_ID = "9cc1e3049173be09";
  private static final String PARENT_SPAN_ID = "8b03ab423da481c5";
  private static final Attributes attributes = Attributes.empty();
  private static final List<Event> annotations =
      ImmutableList.of(
          Event.create(1505855799_433901068L, "RECEIVED", Attributes.empty()),
          Event.create(1505855799_459486280L, "SENT", Attributes.empty()));

  @Test
  void generateSpan_remoteParent() {
    SpanData data = buildStandardSpan().build();

    assertThat(ZipkinSpanExporter.generateSpan(data, localEndpoint))
        .isEqualTo(buildZipkinSpan(Span.Kind.SERVER));
  }

  @Test
  void generateSpan_subMicroDurations() {
    SpanData data =
        buildStandardSpan()
            .setStartEpochNanos(1505855794_194009601L)
            .setEndEpochNanos(1505855794_194009999L)
            .build();

    Span expected = standardZipkinSpanBuilder(Span.Kind.SERVER).duration(1).build();
    assertThat(ZipkinSpanExporter.generateSpan(data, localEndpoint)).isEqualTo(expected);
  }

  @Test
  void generateSpan_ServerKind() {
    SpanData data = buildStandardSpan().setKind(Kind.SERVER).build();

    assertThat(ZipkinSpanExporter.generateSpan(data, localEndpoint))
        .isEqualTo(buildZipkinSpan(Span.Kind.SERVER));
  }

  @Test
  void generateSpan_ClientKind() {
    SpanData data = buildStandardSpan().setKind(Kind.CLIENT).build();

    assertThat(ZipkinSpanExporter.generateSpan(data, localEndpoint))
        .isEqualTo(buildZipkinSpan(Span.Kind.CLIENT));
  }

  @Test
  void generateSpan_InternalKind() {
    SpanData data = buildStandardSpan().setKind(Kind.INTERNAL).build();

    assertThat(ZipkinSpanExporter.generateSpan(data, localEndpoint))
        .isEqualTo(buildZipkinSpan(null));
  }

  @Test
  void generateSpan_ConsumeKind() {
    SpanData data = buildStandardSpan().setKind(Kind.CONSUMER).build();

    assertThat(ZipkinSpanExporter.generateSpan(data, localEndpoint))
        .isEqualTo(buildZipkinSpan(Span.Kind.CONSUMER));
  }

  @Test
  void generateSpan_ProducerKind() {
    SpanData data = buildStandardSpan().setKind(Kind.PRODUCER).build();

    assertThat(ZipkinSpanExporter.generateSpan(data, localEndpoint))
        .isEqualTo(buildZipkinSpan(Span.Kind.PRODUCER));
  }

  @Test
  void generateSpan_ResourceServiceNameMapping() {
    final Resource resource =
        Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, "super-zipkin-service"));
    SpanData data = buildStandardSpan().setResource(resource).build();

    Endpoint expectedEndpoint = Endpoint.newBuilder().serviceName("super-zipkin-service").build();
    Span expectedZipkinSpan =
        buildZipkinSpan(Span.Kind.SERVER).toBuilder().localEndpoint(expectedEndpoint).build();
    assertThat(ZipkinSpanExporter.generateSpan(data, localEndpoint)).isEqualTo(expectedZipkinSpan);
  }

  @Test
  void generateSpan_WithAttributes() {
    Attributes attributes =
        Attributes.builder()
            .setAttribute(stringKey("string"), "string value")
            .setAttribute(booleanKey("boolean"), false)
            .setAttribute(longKey("long"), 9999L)
            .setAttribute(doubleKey("double"), 222.333d)
            .setAttribute(booleanArrayKey("booleanArray"), Arrays.asList(true, false))
            .setAttribute(stringArrayKey("stringArray"), Collections.singletonList("Hello"))
            .setAttribute(doubleArrayKey("doubleArray"), Arrays.asList(32.33d, -98.3d))
            .setAttribute(longArrayKey("longArray"), Arrays.asList(33L, 999L))
            .build();
    SpanData data = buildStandardSpan().setAttributes(attributes).setKind(Kind.CLIENT).build();

    assertThat(ZipkinSpanExporter.generateSpan(data, localEndpoint))
        .isEqualTo(
            buildZipkinSpan(Span.Kind.CLIENT)
                .toBuilder()
                .putTag("string", "string value")
                .putTag("boolean", "false")
                .putTag("long", "9999")
                .putTag("double", "222.333")
                .putTag("booleanArray", "true,false")
                .putTag("stringArray", "Hello")
                .putTag("doubleArray", "32.33,-98.3")
                .putTag("longArray", "33,999")
                .build());
  }

  @Test
  void generateSpan_WithInstrumentationLibraryInfo() {
    SpanData data =
        buildStandardSpan()
            .setInstrumentationLibraryInfo(
                InstrumentationLibraryInfo.create("io.opentelemetry.auto", "1.0.0"))
            .setKind(Kind.CLIENT)
            .build();

    assertThat(ZipkinSpanExporter.generateSpan(data, localEndpoint))
        .isEqualTo(
            buildZipkinSpan(Span.Kind.CLIENT)
                .toBuilder()
                .putTag("otel.library.name", "io.opentelemetry.auto")
                .putTag("otel.library.version", "1.0.0")
                .build());
  }

  @Test
  void generateSpan_AlreadyHasHttpStatusInfo() {
    Attributes attributeMap =
        Attributes.of(
            SemanticAttributes.HTTP_STATUS_CODE, 404L, stringKey("error"), "A user provided error");
    SpanData data =
        buildStandardSpan()
            .setAttributes(attributeMap)
            .setKind(Kind.CLIENT)
            .setStatus(SpanData.Status.error())
            .build();

    assertThat(ZipkinSpanExporter.generateSpan(data, localEndpoint))
        .isEqualTo(
            buildZipkinSpan(Span.Kind.CLIENT)
                .toBuilder()
                .clearTags()
                .putTag(SemanticAttributes.HTTP_STATUS_CODE.getKey(), "404")
                .putTag("error", "A user provided error")
                .build());
  }

  @Test
  void generateSpan_WithRpcErrorStatus() {
    Attributes attributeMap = Attributes.of(SemanticAttributes.RPC_SERVICE, "my service name");

    String errorMessage = "timeout";

    SpanData data =
        buildStandardSpan()
            .setStatus(SpanData.Status.create(StatusCode.ERROR, errorMessage))
            .setAttributes(attributeMap)
            .build();

    assertThat(ZipkinSpanExporter.generateSpan(data, localEndpoint))
        .isEqualTo(
            buildZipkinSpan(Span.Kind.SERVER)
                .toBuilder()
                .putTag(ZipkinSpanExporter.OTEL_STATUS_DESCRIPTION, errorMessage)
                .putTag(SemanticAttributes.RPC_SERVICE.getKey(), "my service name")
                .putTag(ZipkinSpanExporter.OTEL_STATUS_CODE, "ERROR")
                .putTag(ZipkinSpanExporter.STATUS_ERROR.getKey(), "ERROR")
                .build());
  }

  @Test
  void testExport() {
    ZipkinSpanExporter zipkinSpanExporter =
        new ZipkinSpanExporter(mockEncoder, mockSender, "tweetiebird");

    byte[] someBytes = new byte[0];
    when(mockEncoder.encode(buildZipkinSpan(Span.Kind.SERVER))).thenReturn(someBytes);
    when(mockSender.sendSpans(Collections.singletonList(someBytes))).thenReturn(mockZipkinCall);
    doAnswer(
            invocation -> {
              Callback<Void> callback = invocation.getArgument(0);
              callback.onSuccess(null);
              return null;
            })
        .when(mockZipkinCall)
        .enqueue(any());

    CompletableResultCode resultCode =
        zipkinSpanExporter.export(Collections.singleton(buildStandardSpan().build()));

    assertThat(resultCode.isSuccess()).isTrue();
  }

  @Test
  void testExport_failed() {
    ZipkinSpanExporter zipkinSpanExporter =
        new ZipkinSpanExporter(mockEncoder, mockSender, "tweetiebird");

    byte[] someBytes = new byte[0];
    when(mockEncoder.encode(buildZipkinSpan(Span.Kind.SERVER))).thenReturn(someBytes);
    when(mockSender.sendSpans(Collections.singletonList(someBytes))).thenReturn(mockZipkinCall);
    doAnswer(
            invocation -> {
              Callback<Void> callback = invocation.getArgument(0);
              callback.onError(new IOException());
              return null;
            })
        .when(mockZipkinCall)
        .enqueue(any());

    CompletableResultCode resultCode =
        zipkinSpanExporter.export(Collections.singleton(buildStandardSpan().build()));

    assertThat(resultCode.isSuccess()).isFalse();
  }

  @Test
  void testCreate() {
    ZipkinSpanExporter exporter =
        ZipkinSpanExporter.builder().setSender(mockSender).setServiceName("myGreatService").build();

    assertThat(exporter).isNotNull();
  }

  @Test
  void testShutdown() throws IOException {
    ZipkinSpanExporter exporter =
        ZipkinSpanExporter.builder().setServiceName("myGreatService").setSender(mockSender).build();

    exporter.shutdown();
    verify(mockSender).close();
  }

  private static TestSpanData.Builder buildStandardSpan() {
    return TestSpanData.builder()
        .setTraceId(TRACE_ID)
        .setSpanId(SPAN_ID)
        .setParentSpanId(PARENT_SPAN_ID)
        .setSampled(true)
        .setStatus(SpanData.Status.ok())
        .setKind(Kind.SERVER)
        .setHasRemoteParent(true)
        .setName("Recv.helloworld.Greeter.SayHello")
        .setStartEpochNanos(1505855794_194009601L)
        .setEndEpochNanos(1505855799_465726528L)
        .setAttributes(attributes)
        .setTotalAttributeCount(attributes.size())
        .setEvents(annotations)
        .setLinks(Collections.emptyList())
        .setHasEnded(true);
  }

  private static Span buildZipkinSpan(Span.Kind kind) {
    return standardZipkinSpanBuilder(kind).build();
  }

  private static Span.Builder standardZipkinSpanBuilder(Span.Kind kind) {
    return Span.newBuilder()
        .traceId(TRACE_ID)
        .parentId(PARENT_SPAN_ID)
        .id(SPAN_ID)
        .kind(kind)
        .name("Recv.helloworld.Greeter.SayHello")
        .timestamp(1505855794000000L + 194009601L / 1000)
        .duration((1505855799000000L + 465726528L / 1000) - (1505855794000000L + 194009601L / 1000))
        .localEndpoint(localEndpoint)
        .addAnnotation(1505855799000000L + 433901068L / 1000, "RECEIVED")
        .addAnnotation(1505855799000000L + 459486280L / 1000, "SENT");
  }

  abstract static class ConfigBuilderTest extends ConfigBuilder<ConfigBuilderTest> {
    public static NamingConvention getNaming() {
      return NamingConvention.DOT;
    }
  }

  @Test
  void configTest() {
    Map<String, String> options = new HashMap<>();
    String serviceName = "myGreatService";
    String endpoint = "http://127.0.0.1:9090";
    options.put("otel.exporter.zipkin.service.name", serviceName);
    options.put("otel.exporter.zipkin.endpoint", endpoint);
    ZipkinSpanExporter.Builder config = ZipkinSpanExporter.builder();
    ZipkinSpanExporter.Builder spy = Mockito.spy(config);
    spy.fromConfigMap(options, ConfigBuilderTest.getNaming()).build();
    Mockito.verify(spy).setServiceName(serviceName);
    Mockito.verify(spy).setEndpoint(endpoint);
  }
}
