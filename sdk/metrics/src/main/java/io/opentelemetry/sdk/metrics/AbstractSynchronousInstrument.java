/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.metrics;

import io.opentelemetry.common.Labels;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.metrics.data.MetricData;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

abstract class AbstractSynchronousInstrument<B extends AbstractBoundInstrument>
    extends AbstractInstrument {
  private final ConcurrentHashMap<Labels, B> boundLabels;
  private final ReentrantLock collectLock;

  AbstractSynchronousInstrument(
      InstrumentDescriptor descriptor,
      MeterProviderSharedState meterProviderSharedState,
      MeterSharedState meterSharedState,
      ActiveBatcher activeBatcher) {
    super(descriptor, meterProviderSharedState, meterSharedState, activeBatcher);
    boundLabels = new ConcurrentHashMap<>();
    collectLock = new ReentrantLock();
  }

  public B bind(Labels labels) {
    Objects.requireNonNull(labels, "labels");
    for (MetricsProcessor p : getMeterSharedState().getMetricsProcessors()) {
      labels = p.onLabelsBound(Context.current(), this, labels);
    }
    B binding = boundLabels.get(labels);
    if (binding != null && binding.bind()) {
      // At this moment it is guaranteed that the Bound is in the map and will not be removed.
      return binding;
    }

    // Missing entry or no longer mapped, try to add a new entry.
    binding = newBinding(getActiveBatcher());
    while (true) {
      B oldBound = boundLabels.putIfAbsent(labels, binding);
      if (oldBound != null) {
        if (oldBound.bind()) {
          // At this moment it is guaranteed that the Bound is in the map and will not be removed.
          return oldBound;
        }
        // Try to remove the oldBound. This will race with the collect method, but only one will
        // succeed.
        boundLabels.remove(labels, oldBound);
        continue;
      }
      return binding;
    }
  }

  /**
   * Collects records from all the entries (labelSet, Bound) that changed since the last collect()
   * call.
   */
  @Override
  final List<MetricData> collectAll() {
    collectLock.lock();
    try {
      Batcher batcher = getActiveBatcher();
      for (Map.Entry<Labels, B> entry : boundLabels.entrySet()) {
        boolean unmappedEntry = entry.getValue().tryUnmap();
        if (unmappedEntry) {
          // If able to unmap then remove the record from the current Map. This can race with the
          // acquire but because we requested a specific value only one will succeed.
          boundLabels.remove(entry.getKey(), entry.getValue());
        }
        batcher.batch(entry.getKey(), entry.getValue().getAggregator(), unmappedEntry);
      }
      return batcher.completeCollectionCycle();
    } finally {
      collectLock.unlock();
    }
  }

  abstract B newBinding(Batcher batcher);
}
