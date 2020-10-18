/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.example.http;

import static io.opentelemetry.common.AttributeKey.stringKey;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.grpc.Context;
import io.opentelemetry.OpenTelemetry;
import io.opentelemetry.common.Attributes;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.exporters.logging.LoggingSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.TracerSdkManagement;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.trace.Span;
import io.opentelemetry.trace.Tracer;
import io.opentelemetry.trace.TracingContextUtils;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;

public class HttpServer {

  private static class HelloHandler implements HttpHandler {

    @Override
    public void handle(HttpExchange exchange) throws IOException {
      // Name convention for the Span is not yet defined.
      // See: https://github.com/open-telemetry/opentelemetry-specification/issues/270

      // Extract the context from the HTTP request
      Context context =
          OpenTelemetry.getPropagators()
              .getTextMapPropagator()
              .extract(Context.current(), exchange, getter);

      Span span =
          tracer.spanBuilder("/").setParent(context).setSpanKind(Span.Kind.SERVER).startSpan();

      try (Scope scope = TracingContextUtils.currentContextWith(span)) {
        // Set the Semantic Convention
        span.setAttribute("component", "http");
        span.setAttribute("http.method", "GET");
        /*
         One of the following is required:
         - http.scheme, http.host, http.target
         - http.scheme, http.server_name, net.host.port, http.target
         - http.scheme, net.host.name, net.host.port, http.target
         - http.url
        */
        span.setAttribute("http.scheme", "http");
        span.setAttribute("http.host", "localhost:" + HttpServer.port);
        span.setAttribute("http.target", "/");
        // Process the request
        answer(exchange, span);
      } finally {
        // Close the span
        span.end();
      }
    }

    private void answer(HttpExchange he, Span span) throws IOException {
      // Generate an Event
      span.addEvent("Start Processing");

      // Process the request
      String response = "Hello World!";
      he.sendResponseHeaders(200, response.length());
      OutputStream os = he.getResponseBody();
      os.write(response.getBytes(Charset.defaultCharset()));
      os.close();
      System.out.println("Served Client: " + he.getRemoteAddress());

      // Generate an Event with an attribute
      Attributes eventAttributes = Attributes.of(stringKey("answer"), response);
      span.addEvent("Finish Processing", eventAttributes);
    }
  }

  private final com.sun.net.httpserver.HttpServer server;
  private static final int port = 8080;

  // OTel API
  private static final Tracer tracer =
      OpenTelemetry.getTracer("io.opentelemetry.example.http.HttpServer");
  // Export traces to log
  private static final LoggingSpanExporter loggingExporter = new LoggingSpanExporter();
  // Extract the context from http headers
  private static final TextMapPropagator.Getter<HttpExchange> getter =
      (carrier, key) -> {
        if (carrier.getRequestHeaders().containsKey(key)) {
          return carrier.getRequestHeaders().get(key).get(0);
        }
        return "";
      };

  private HttpServer() throws IOException {
    this(port);
  }

  private HttpServer(int port) throws IOException {
    initTracer();
    server = com.sun.net.httpserver.HttpServer.create(new InetSocketAddress(port), 0);
    // Test urls
    server.createContext("/", new HelloHandler());
    server.start();
    System.out.println("Server ready on http://127.0.0.1:" + port);
  }

  private void initTracer() {
    // Get the tracer
    TracerSdkManagement tracerManagement = OpenTelemetrySdk.getTracerManagement();
    // Show that multiple exporters can be used

    // Set to export the traces also to a log file
    tracerManagement.addSpanProcessor(SimpleSpanProcessor.newBuilder(loggingExporter).build());
  }

  private void stop() {
    server.stop(0);
  }

  /**
   * Main method to run the example.
   *
   * @param args It is not required.
   * @throws Exception Something might go wrong.
   */
  public static void main(String[] args) throws Exception {
    final HttpServer s = new HttpServer();
    // Gracefully close the server
    Runtime.getRuntime().addShutdownHook(new Thread(s::stop));
  }
}
