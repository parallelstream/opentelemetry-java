/*
 * Copyright 2020, OpenTelemetry Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.opentelemetry.sdk.metrics;

import io.grpc.Context;
import io.opentelemetry.common.Labels;
import io.opentelemetry.correlationcontext.CorrelationContext;
import io.opentelemetry.correlationcontext.CorrelationsContextUtils;
import io.opentelemetry.sdk.metrics.data.MetricData;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

abstract class AbstractSynchronousInstrument<B extends AbstractBoundInstrument>
    extends AbstractInstrument {
  private final ConcurrentHashMap<Labels, B> boundLabels;
  private final ReentrantLock collectLock;

  private final Set<String> stickyLabelsPrefixes;

  AbstractSynchronousInstrument(
      InstrumentDescriptor descriptor,
      MeterProviderSharedState meterProviderSharedState,
      MeterSharedState meterSharedState,
      ActiveBatcher activeBatcher) {
    super(descriptor, meterProviderSharedState, meterSharedState, activeBatcher);
    this.stickyLabelsPrefixes = this.getMeterProviderSharedState().stickyLabelsPrefixes();
    boundLabels = new ConcurrentHashMap<>();
    collectLock = new ReentrantLock();
  }

  private Labels combineWithSticky(Labels labels) {
    if (stickyLabelsPrefixes.isEmpty())
      return labels;
    Labels.Builder builder = labels.toBuilder();
    CorrelationContext ctx = CorrelationsContextUtils.getCorrelationContext(Context.current());
    ctx.getEntries().stream().filter(e -> stickyLabelsPrefixes.contains(e.getKey().substring(0, e.getKey().indexOf('.'))))
        .forEach(e -> builder.setLabel(e.getKey(), e.getValue()));
    return builder.build();
  }

  public B bind(Labels labels) {
    Objects.requireNonNull(labels, "labels");
    labels = combineWithSticky(labels);
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

  abstract static class Builder<B extends AbstractSynchronousInstrument.Builder<?>> extends
      AbstractInstrument.Builder<B> {


    private final Set<String> stickyLabelsPrefixes;

    public Set<String> getStickyLabelsPrefixes() {
      return stickyLabelsPrefixes;
    }

    Builder(String name, MeterProviderSharedState meterProviderSharedState,
        MeterSharedState meterSharedState, MeterSdk meterSdk,
        Set<String> stickyLabelsPrefixes) {
      super(name, meterProviderSharedState, meterSharedState, meterSdk);
      this.stickyLabelsPrefixes = stickyLabelsPrefixes;
    }


  }
}
