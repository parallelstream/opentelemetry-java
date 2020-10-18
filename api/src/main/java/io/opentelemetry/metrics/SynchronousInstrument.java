/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.metrics;

import io.opentelemetry.common.Labels;
import io.opentelemetry.metrics.SynchronousInstrument.BoundInstrument;
import javax.annotation.concurrent.ThreadSafe;

/**
 * SynchronousInstrument is an interface that defines a type of instruments that are used to report
 * measurements synchronously. That is, when the user reports individual measurements as they occur.
 *
 * <p>Synchronous instrument events additionally have a Context associated with them, describing
 * properties of the associated trace and distributed correlation values.
 *
 * @param <B> the specific type of Bound Instrument this instrument can provide.
 */
@ThreadSafe
public interface SynchronousInstrument<B extends BoundInstrument> extends Instrument {
  /**
   * Returns a {@code Bound Instrument} associated with the specified labels. Multiples requests
   * with the same set of labels may return the same {@code Bound Instrument} instance.
   *
   * <p>It is recommended that callers keep a reference to the Bound Instrument instead of always
   * calling this method for every operation.
   *
   * @param labels the set of labels, as key-value pairs.
   * @return a {@code Bound Instrument}
   * @throws NullPointerException if {@code labelValues} is null.
   */
  B bind(Labels labels);

  interface BoundInstrument {
    /**
     * Unbinds the current {@code Bound} from the Instrument.
     *
     * <p>After this method returns the current instance {@code Bound} is considered invalid (not
     * being managed by the instrument).
     */
    void unbind();
  }

  /** Builder class for {@link SynchronousInstrument}. */
  interface Builder extends Instrument.Builder {
    @Override
    SynchronousInstrument<?> build();
  }
}
