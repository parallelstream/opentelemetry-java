/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.exporters.otlp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

import com.google.common.io.Closer;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.common.Labels;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.sdk.common.InstrumentationLibraryInfo;
import io.opentelemetry.sdk.common.export.ConfigBuilder;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricData.LongPoint;
import io.opentelemetry.sdk.resources.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class OtlpGrpcMetricExporterTest {

  private final FakeCollector fakeCollector = new FakeCollector();
  private final String serverName = InProcessServerBuilder.generateName();
  private final ManagedChannel inProcessChannel =
      InProcessChannelBuilder.forName(serverName).directExecutor().build();

  abstract static class ConfigBuilderTest extends ConfigBuilder<ConfigBuilderTest> {
    public static NamingConvention getNaming() {
      return NamingConvention.DOT;
    }
  }

  private final Closer closer = Closer.create();

  @BeforeEach
  public void setup() throws IOException {
    Server server =
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(fakeCollector)
            .build()
            .start();
    closer.register(server::shutdownNow);
    closer.register(inProcessChannel::shutdownNow);
  }

  @AfterEach
  void tearDown() throws Exception {
    closer.close();
  }

  @Test
  void configTest() {
    Map<String, String> options = new HashMap<>();
    options.put("otel.exporter.otlp.metric.timeout", "12");
    options.put("otel.exporter.otlp.insecure", "true");
    OtlpGrpcMetricExporter.Builder config = OtlpGrpcMetricExporter.builder();
    OtlpGrpcMetricExporter.Builder spy = Mockito.spy(config);
    spy.fromConfigMap(options, ConfigBuilderTest.getNaming());
    verify(spy).setDeadlineMs(12);
    verify(spy).setUseTls(false);
  }

  @Test
  void testExport() {
    MetricData span = generateFakeMetric();
    OtlpGrpcMetricExporter exporter =
        OtlpGrpcMetricExporter.builder().setChannel(inProcessChannel).build();
    try {
      assertThat(exporter.export(Collections.singletonList(span)).isSuccess()).isTrue();
      assertThat(fakeCollector.getReceivedMetrics())
          .isEqualTo(MetricAdapter.toProtoResourceMetrics(Collections.singletonList(span)));
    } finally {
      exporter.shutdown();
    }
  }

  @Test
  void testExport_MultipleMetrics() {
    List<MetricData> spans = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      spans.add(generateFakeMetric());
    }
    OtlpGrpcMetricExporter exporter =
        OtlpGrpcMetricExporter.builder().setChannel(inProcessChannel).build();
    try {
      assertThat(exporter.export(spans).isSuccess()).isTrue();
      assertThat(fakeCollector.getReceivedMetrics())
          .isEqualTo(MetricAdapter.toProtoResourceMetrics(spans));
    } finally {
      exporter.shutdown();
    }
  }

  @Test
  void testExport_DeadlineSetPerExport() {
    int deadlineMs = 1000;
    OtlpGrpcMetricExporter exporter =
        OtlpGrpcMetricExporter.builder()
            .setChannel(inProcessChannel)
            .setDeadlineMs(deadlineMs)
            .build();

    try {
      fakeCollector.setCollectorDelay(2000);
      assertThat(exporter.export(Collections.singletonList(generateFakeMetric())).isSuccess())
          .isFalse();

      fakeCollector.setCollectorDelay(0);
      assertThat(exporter.export(Collections.singletonList(generateFakeMetric())).isSuccess())
          .isTrue();
    } finally {
      exporter.shutdown();
    }
  }

  @Test
  void testExport_AfterShutdown() {
    MetricData span = generateFakeMetric();
    OtlpGrpcMetricExporter exporter =
        OtlpGrpcMetricExporter.builder().setChannel(inProcessChannel).build();
    exporter.shutdown();
    assertThat(exporter.export(Collections.singletonList(span)).isSuccess()).isFalse();
  }

  @Test
  void testExport_Cancelled() {
    fakeCollector.setReturnedStatus(Status.CANCELLED);
    OtlpGrpcMetricExporter exporter =
        OtlpGrpcMetricExporter.builder().setChannel(inProcessChannel).build();
    try {
      assertThat(exporter.export(Collections.singletonList(generateFakeMetric())).isSuccess())
          .isFalse();
    } finally {
      exporter.shutdown();
    }
  }

  @Test
  void testExport_DeadlineExceeded() {
    fakeCollector.setReturnedStatus(Status.DEADLINE_EXCEEDED);
    OtlpGrpcMetricExporter exporter =
        OtlpGrpcMetricExporter.builder().setChannel(inProcessChannel).build();
    try {
      assertThat(exporter.export(Collections.singletonList(generateFakeMetric())).isSuccess())
          .isFalse();
    } finally {
      exporter.shutdown();
    }
  }

  @Test
  void testExport_ResourceExhausted() {
    fakeCollector.setReturnedStatus(Status.RESOURCE_EXHAUSTED);
    OtlpGrpcMetricExporter exporter =
        OtlpGrpcMetricExporter.builder().setChannel(inProcessChannel).build();
    try {
      assertThat(exporter.export(Collections.singletonList(generateFakeMetric())).isSuccess())
          .isFalse();
    } finally {
      exporter.shutdown();
    }
  }

  @Test
  void testExport_OutOfRange() {
    fakeCollector.setReturnedStatus(Status.OUT_OF_RANGE);
    OtlpGrpcMetricExporter exporter =
        OtlpGrpcMetricExporter.builder().setChannel(inProcessChannel).build();
    try {
      assertThat(exporter.export(Collections.singletonList(generateFakeMetric())).isSuccess())
          .isFalse();
    } finally {
      exporter.shutdown();
    }
  }

  @Test
  void testExport_Unavailable() {
    fakeCollector.setReturnedStatus(Status.UNAVAILABLE);
    OtlpGrpcMetricExporter exporter =
        OtlpGrpcMetricExporter.builder().setChannel(inProcessChannel).build();
    try {
      assertThat(exporter.export(Collections.singletonList(generateFakeMetric())).isSuccess())
          .isFalse();
    } finally {
      exporter.shutdown();
    }
  }

  @Test
  void testExport_DataLoss() {
    fakeCollector.setReturnedStatus(Status.DATA_LOSS);
    OtlpGrpcMetricExporter exporter =
        OtlpGrpcMetricExporter.builder().setChannel(inProcessChannel).build();
    try {
      assertThat(exporter.export(Collections.singletonList(generateFakeMetric())).isSuccess())
          .isFalse();
    } finally {
      exporter.shutdown();
    }
  }

  @Test
  void testExport_PermissionDenied() {
    fakeCollector.setReturnedStatus(Status.PERMISSION_DENIED);
    OtlpGrpcMetricExporter exporter =
        OtlpGrpcMetricExporter.builder().setChannel(inProcessChannel).build();
    try {
      assertThat(exporter.export(Collections.singletonList(generateFakeMetric())).isSuccess())
          .isFalse();
    } finally {
      exporter.shutdown();
    }
  }

  @Test
  void testExport_flush() {
    OtlpGrpcMetricExporter exporter =
        OtlpGrpcMetricExporter.builder().setChannel(inProcessChannel).build();
    try {
      assertThat(exporter.flush().isSuccess()).isTrue();
    } finally {
      exporter.shutdown();
    }
  }

  private static MetricData generateFakeMetric() {
    long startNs = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis());
    long endNs = startNs + TimeUnit.MILLISECONDS.toNanos(900);
    return MetricData.create(
        Resource.getEmpty(),
        InstrumentationLibraryInfo.getEmpty(),
        "name",
        "description",
        "1",
        MetricData.Type.MONOTONIC_LONG,
        Collections.singletonList(LongPoint.create(startNs, endNs, Labels.of("k", "v"), 5)));
  }

  private static final class FakeCollector extends MetricsServiceGrpc.MetricsServiceImplBase {
    private final List<ResourceMetrics> receivedMetrics = new ArrayList<>();
    private Status returnedStatus = Status.OK;
    private long delayMs = 0;

    @Override
    public void export(
        ExportMetricsServiceRequest request,
        StreamObserver<ExportMetricsServiceResponse> responseObserver) {

      if (delayMs > 0) {
        try {
          // add a delay to simulate export taking a long time
          TimeUnit.MILLISECONDS.sleep(delayMs);
        } catch (InterruptedException e) {
          // do nothing
        }
      }

      receivedMetrics.addAll(request.getResourceMetricsList());
      responseObserver.onNext(ExportMetricsServiceResponse.newBuilder().build());
      if (!returnedStatus.isOk()) {
        if (returnedStatus.getCode() == Code.DEADLINE_EXCEEDED) {
          // Do not call onCompleted to simulate a deadline exceeded.
          return;
        }
        responseObserver.onError(returnedStatus.asRuntimeException());
        return;
      }
      responseObserver.onCompleted();
    }

    List<ResourceMetrics> getReceivedMetrics() {
      return receivedMetrics;
    }

    void setReturnedStatus(Status returnedStatus) {
      this.returnedStatus = returnedStatus;
    }

    void setCollectorDelay(long delayMs) {
      this.delayMs = delayMs;
    }
  }
}
