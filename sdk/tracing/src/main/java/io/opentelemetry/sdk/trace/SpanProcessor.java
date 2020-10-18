/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.trace;

import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.trace.Span;

/**
 * SpanProcessor is the interface {@code TracerSdk} uses to allow synchronous hooks for when a
 * {@code Span} is started or when a {@code Span} is ended.
 */
public interface SpanProcessor {
  /**
   * Called when a {@link io.opentelemetry.trace.Span} is started, if the {@link Span#isRecording()}
   * returns true.
   *
   * <p>This method is called synchronously on the execution thread, should not throw or block the
   * execution thread.
   *
   * @param span the {@code ReadableSpan} that just started.
   * @param parentContext the parent {@code Context} of the span that just started.
   */
  void onStart(ReadWriteSpan span, Context parentContext);

  /**
   * Returns {@code true} if this {@link SpanProcessor} requires start events.
   *
   * @return {@code true} if this {@link SpanProcessor} requires start events.
   */
  boolean isStartRequired();

  /**
   * Called when a {@link io.opentelemetry.trace.Span} is ended, if the {@link Span#isRecording()}
   * returns true.
   *
   * <p>This method is called synchronously on the execution thread, should not throw or block the
   * execution thread.
   *
   * @param span the {@code ReadableSpan} that just ended.
   */
  void onEnd(ReadableSpan span);

  /**
   * Returns {@code true} if this {@link SpanProcessor} requires end events.
   *
   * @return {@code true} if this {@link SpanProcessor} requires end events.
   */
  boolean isEndRequired();

  /**
   * Processes all span events that have not yet been processed and closes used resources.
   *
   * @return a {@link CompletableResultCode} which completes when shutdown is finished.
   */
  CompletableResultCode shutdown();

  /**
   * Processes all span events that have not yet been processed.
   *
   * @return a {@link CompletableResultCode} which completes when currently queued spans are
   *     finished processing.
   */
  CompletableResultCode forceFlush();
}
