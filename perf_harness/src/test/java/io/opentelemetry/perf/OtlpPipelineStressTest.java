/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.perf;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import eu.rekawek.toxiproxy.model.ToxicList;
import eu.rekawek.toxiproxy.model.toxic.Timeout;
import io.opentelemetry.OpenTelemetry;
import io.opentelemetry.common.Labels;
import io.opentelemetry.context.Scope;
import io.opentelemetry.exporters.inmemory.InMemoryMetricExporter;
import io.opentelemetry.exporters.otlp.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricData.LongPoint;
import io.opentelemetry.sdk.metrics.data.MetricData.Point;
import io.opentelemetry.sdk.metrics.export.IntervalMetricReader;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.trace.Span;
import io.opentelemetry.trace.Tracer;
import io.opentelemetry.trace.TracingContextUtils;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Ignore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

@Testcontainers(disabledWithoutDocker = true)
@SuppressWarnings({"FutureReturnValueIgnored", "CatchAndPrintStackTrace"})
public class OtlpPipelineStressTest {

  public static final int OTLP_RECEIVER_PORT = 55680;
  public static final int COLLECTOR_PROXY_PORT = 44444;
  public static final int TOXIPROXY_CONTROL_PORT = 8474;
  public static Network network = Network.newNetwork();
  public static AtomicLong totalSpansReceivedByCollector = new AtomicLong();

  @Container
  public static GenericContainer<?> collectorContainer =
      new GenericContainer<>(DockerImageName.parse("otel/opentelemetry-collector-dev:latest"))
          .withNetwork(network)
          .withNetworkAliases("otel-collector")
          .withExposedPorts(OTLP_RECEIVER_PORT)
          .withCommand("--config=/etc/otel-collector-config-perf.yaml")
          .withCopyFileToContainer(
              MountableFile.forClasspathResource("otel-collector-config-perf.yaml"),
              "/etc/otel-collector-config-perf.yaml")
          .withLogConsumer(outputFrame -> System.out.print(outputFrame.getUtf8String()))
          .withLogConsumer(
              outputFrame -> {
                String logline = outputFrame.getUtf8String();
                String spanExportPrefix = "TraceExporter\t{\"#spans\": ";
                int start = logline.indexOf(spanExportPrefix);
                int end = logline.indexOf("}");
                if (start > 0) {
                  String substring = logline.substring(start + spanExportPrefix.length(), end);
                  totalSpansReceivedByCollector.addAndGet(Long.parseLong(substring));
                }
              })
          .waitingFor(new LogMessageWaitStrategy().withRegEx(".*Everything is ready.*"));

  @Container
  public static GenericContainer<?> toxiproxyContainer =
      new GenericContainer<>(DockerImageName.parse("shopify/toxiproxy:latest"))
          .withNetwork(network)
          .withNetworkAliases("toxiproxy")
          .withExposedPorts(TOXIPROXY_CONTROL_PORT, COLLECTOR_PROXY_PORT)
          .dependsOn(collectorContainer)
          //          .withLogConsumer(outputFrame -> System.out.print(outputFrame.getUtf8String()))
          .waitingFor(new LogMessageWaitStrategy().withRegEx(".*API HTTP server starting.*"));

  private final InMemoryMetricExporter metricExporter = InMemoryMetricExporter.create();

  private IntervalMetricReader intervalMetricReader;
  private Proxy collectorProxy;
  private ToxiproxyClient toxiproxyClient;

  @BeforeEach
  void setUp() throws IOException {
    toxiproxyClient =
        new ToxiproxyClient(
            toxiproxyContainer.getHost(), toxiproxyContainer.getMappedPort(TOXIPROXY_CONTROL_PORT));
    toxiproxyClient.reset();
    collectorProxy = toxiproxyClient.getProxyOrNull("collector");

    if (collectorProxy == null) {
      collectorProxy =
          toxiproxyClient.createProxy(
              "collector",
              "0.0.0.0:" + COLLECTOR_PROXY_PORT,
              "otel-collector" + ":" + OTLP_RECEIVER_PORT);
    }
    collectorProxy.enable();

    intervalMetricReader = setupSdk(metricExporter);
    addOtlpSpanExporter();
  }

  @AfterEach
  void tearDown() throws IOException {
    intervalMetricReader.shutdown();
    OpenTelemetrySdk.getTracerManagement().shutdown();

    toxiproxyClient.reset();
    collectorProxy.delete();
    System.out.println("totalSpansReceivedByCollector = " + totalSpansReceivedByCollector);
  }

  @Test
  @Ignore("we don't want to run this with every build.")
  void oltpExportWithFlakyCollector() throws IOException, InterruptedException {
    ToxicList toxics = collectorProxy.toxics();
    //    Latency latency = toxics.latency("jittery_latency", ToxicDirection.UPSTREAM, 500);
    //    latency.setJitter(1000);
    //    latency.setToxicity(0.4f);
    //    for (Toxic toxic : toxiproxyClient.getProxy("collector").toxics().getAll()) {
    //      System.out.println("toxic = " + toxic.getName());
    //    }

    // warm up with a fixed 1000 spans
    runOnce(1000, 0);
    Thread.sleep(2000);
    metricExporter.reset();

    // spawn threads that will each run for an interval of time
    int numberOfThreads = 8;
    ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);
    CountDownLatch latch = new CountDownLatch(1);
    for (int i = 0; i < numberOfThreads; i++) {
      executorService.submit(
          () -> {
            try {
              latch.await();
              runOnce(null, 30_000);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          });
    }
    latch.countDown();

    // timeout the connection after 5s, then allow reconnecting
    Thread.sleep(5000);
    Timeout timeout = toxics.timeout("timeout_connection", ToxicDirection.UPSTREAM, 1000);
    // wait a second before allowing new connections
    Thread.sleep(1000);
    timeout.remove();

    executorService.shutdown();
    executorService.awaitTermination(1, TimeUnit.MINUTES);

    Thread.sleep(10000);
    List<MetricData> finishedMetricItems = metricExporter.getFinishedMetricItems();
    intervalMetricReader.shutdown();
    Thread.sleep(1000);
    reportMetrics(finishedMetricItems);
    Thread.sleep(10000);
  }

  private static void reportMetrics(List<MetricData> finishedMetricItems) {
    Map<String, List<MetricData>> metricsByName =
        finishedMetricItems.stream().collect(Collectors.groupingBy(MetricData::getName));
    metricsByName.forEach(
        (name, metricData) -> {
          Stream<LongPoint> longPointStream =
              metricData.stream().flatMap(md -> md.getPoints().stream()).map(p -> (LongPoint) p);
          Map<Labels, List<LongPoint>> pointsByLabelset =
              longPointStream.collect(Collectors.groupingBy(Point::getLabels));
          pointsByLabelset.forEach(
              (labels, longPoints) -> {
                long total = longPoints.get(longPoints.size() - 1).getValue();
                System.out.println(name + " : " + labels + " : " + total);
              });
        });
  }

  private static void runOnce(Integer numberOfSpans, int numberOfMillisToRunFor)
      throws InterruptedException {
    Tracer tracer = OpenTelemetry.getTracer("io.opentelemetry.perf");
    long start = System.currentTimeMillis();
    int i = 0;
    while (numberOfSpans == null
        ? System.currentTimeMillis() - start < numberOfMillisToRunFor
        : i < numberOfSpans) {
      //    for (int i = 0; i < 10000; i++) {
      Span exampleSpan = tracer.spanBuilder("exampleSpan").startSpan();
      try (Scope scope = TracingContextUtils.currentContextWith(exampleSpan)) {
        exampleSpan.setAttribute("exampleNumber", i++);
        exampleSpan.setAttribute("attribute0", "attvalue-0");
        exampleSpan.setAttribute("attribute1", "attvalue-1");
        exampleSpan.setAttribute("attribute2", "attvalue-2");
        exampleSpan.setAttribute("attribute3", "attvalue-3");
        exampleSpan.setAttribute("attribute4", "attvalue-4");
        exampleSpan.setAttribute("attribute5", "attvalue-5");
        exampleSpan.setAttribute("attribute6", "attvalue-6");
        exampleSpan.setAttribute("attribute7", "attvalue-7");
        exampleSpan.setAttribute("attribute8", "attvalue-8");
        exampleSpan.setAttribute("attribute9", "attvalue-9");
        exampleSpan.addEvent("pre-sleep");
        Thread.sleep(1);
      } finally {
        exampleSpan.end();
      }
    }
  }

  private static IntervalMetricReader setupSdk(MetricExporter metricExporter) {
    // this will make sure that a proper service.name attribute is set on all the spans/metrics.
    // note: this is not something you should generally do in code, but should be provided on the
    // command-line. This is here to make the example more self-contained.
    System.setProperty(
        "otel.resource.attributes", "service.name=PerfTester,service.version=1.0.1-RC-1");

    // set up the metric exporter and wire it into the SDK and a timed reader.

    IntervalMetricReader intervalMetricReader =
        IntervalMetricReader.builder()
            .setMetricExporter(metricExporter)
            .setMetricProducers(
                Collections.singleton(OpenTelemetrySdk.getMeterProvider().getMetricProducer()))
            .setExportIntervalMillis(1000)
            .build();
    return intervalMetricReader;
  }

  private static void addOtlpSpanExporter() {
    // set up the span exporter and wire it into the SDK
    OtlpGrpcSpanExporter spanExporter =
        OtlpGrpcSpanExporter.builder()
            .setEndpoint(
                toxiproxyContainer.getHost()
                    + ":"
                    + toxiproxyContainer.getMappedPort(COLLECTOR_PROXY_PORT))
            //            .setDeadlineMs(1000)
            .build();
    BatchSpanProcessor spanProcessor =
        BatchSpanProcessor.builder(spanExporter)
            //            .setMaxQueueSize(1000)
            //            .setMaxExportBatchSize(1024)
            //            .setScheduleDelayMillis(1000)
            .build();
    OpenTelemetrySdk.getTracerManagement().addSpanProcessor(spanProcessor);
  }
}
