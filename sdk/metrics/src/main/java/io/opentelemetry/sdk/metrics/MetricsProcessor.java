package io.opentelemetry.sdk.metrics;

import io.opentelemetry.common.Labels;
import io.opentelemetry.context.Context;

public interface MetricsProcessor {
  /**
   * Called when bind() method is called. Allows to manipulate labels which this
   * instrument is bound to. Particular use case includes enriching lables and/or adding more labels
   * depending on the Context
   *
   * @param ctx context of the operation
   * @param descriptor instrument descriptor
   * @param labels immutable labels. When processors are chained output labels of the previous one is passed as
   *     an input to the next one. Last labels returned by a chain of processors are used for bind() operation.
   * @return labels to be used as an input to the next processor in chain or bind() operation if this is the last processor
   */
  Labels onLabelsBound(Context ctx, InstrumentDescriptor descriptor, Labels labels);

  /**
   * Called when .record() method is called. Allows to manipulate recorded value. When chained input of the next
   * call is the output of the previous call. Final output is recorded
   *
   * @param ctx context of the operation
   * @param descriptor instrument descriptor
   * @param labels immutable labels. When processors are chained output labels of the previous one is passed as
   * @param value recorded value
   * @return value to be used as an input to the next processor in chain or record() operation if this is the last processor
   */

  long onLongMeasurement(Context ctx, InstrumentDescriptor descriptor, Labels labels, long value);

  double onDoubleMeasurement(Context ctx, InstrumentDescriptor descriptor, Labels labels,
      double value);
}

