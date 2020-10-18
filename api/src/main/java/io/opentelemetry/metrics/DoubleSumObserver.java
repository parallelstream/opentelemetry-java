/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.metrics;

import io.opentelemetry.metrics.AsynchronousInstrument.DoubleResult;
import javax.annotation.concurrent.ThreadSafe;

/**
 * {@code SumObserver} is the asynchronous instrument corresponding to Counter, used to capture a
 * monotonic sum with Observe(sum).
 *
 * <p>"Sum" appears in the name to remind that it is used to capture sums directly. Use a
 * SumObserver to capture any value that starts at zero and rises throughout the process lifetime
 * and never falls.
 *
 * <p>A {@code SumObserver} is a good choice in situations where a measurement is expensive to
 * compute, such that it would be wasteful to compute on every request.
 *
 * <p>Example:
 *
 * <pre>{@code
 * class YourClass {
 *
 *   private static final Meter meter = OpenTelemetry.getMeterProvider().get("my_library_name");
 *   private static final DoubleSumObserver cpuObserver =
 *       meter.
 *           .doubleSumObserverBuilder("cpu_time")
 *           .setDescription("System CPU usage")
 *           .setUnit("ms")
 *           .build();
 *
 *   void init() {
 *     cpuObserver.setCallback(
 *         new DoubleSumObserver.Callback<DoubleResult>() {
 *          {@literal @}Override
 *           public void update(DoubleResult result) {
 *             // Get system cpu usage
 *             result.observe(cpuIdle, "state", "idle");
 *             result.observe(cpuUser, "state", "user");
 *           }
 *         });
 *   }
 * }
 * }</pre>
 */
@ThreadSafe
public interface DoubleSumObserver extends AsynchronousInstrument<DoubleResult> {
  @Override
  void setCallback(Callback<DoubleResult> callback);

  /** Builder class for {@link DoubleSumObserver}. */
  interface Builder extends AsynchronousInstrument.Builder {
    @Override
    Builder setDescription(String description);

    @Override
    Builder setUnit(String unit);

    @Override
    DoubleSumObserver build();
  }
}
