/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.metrics;

import io.opentelemetry.common.Labels;
import io.opentelemetry.internal.StringUtils;
import io.opentelemetry.internal.Utils;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

/** No-op implementations of {@link Meter}. */
@ThreadSafe
public final class DefaultMeter implements Meter {

  private static final DefaultMeter INSTANCE = new DefaultMeter();
  private static final String COUNTERS_CAN_ONLY_INCREASE = "Counters can only increase";

  /* VisibleForTesting */ static final String ERROR_MESSAGE_INVALID_NAME =
      "Name should be a ASCII string with a length no greater than "
          + StringUtils.METRIC_NAME_MAX_LENGTH
          + " characters.";

  /**
   * Returns a {@code Meter} singleton that is the default implementations for {@link Meter}.
   *
   * @return a {@code Meter} singleton that is the default implementations for {@link Meter}.
   */
  public static Meter getInstance() {
    return INSTANCE;
  }

  @Override
  public DoubleCounter.Builder doubleCounterBuilder(String name) {
    Objects.requireNonNull(name, "name");
    Utils.checkArgument(StringUtils.isValidMetricName(name), ERROR_MESSAGE_INVALID_NAME);
    return new NoopDoubleCounter.NoopBuilder();
  }

  @Override
  public LongCounter.Builder longCounterBuilder(String name) {
    Objects.requireNonNull(name, "name");
    Utils.checkArgument(StringUtils.isValidMetricName(name), ERROR_MESSAGE_INVALID_NAME);
    return new NoopLongCounter.NoopBuilder();
  }

  @Override
  public DoubleUpDownCounter.Builder doubleUpDownCounterBuilder(String name) {
    Objects.requireNonNull(name, "name");
    Utils.checkArgument(StringUtils.isValidMetricName(name), ERROR_MESSAGE_INVALID_NAME);
    return new NoopDoubleUpDownCounter.NoopBuilder();
  }

  @Override
  public LongUpDownCounter.Builder longUpDownCounterBuilder(String name) {
    Objects.requireNonNull(name, "name");
    Utils.checkArgument(StringUtils.isValidMetricName(name), ERROR_MESSAGE_INVALID_NAME);
    return new NoopLongUpDownCounter.NoopBuilder();
  }

  @Override
  public DoubleValueRecorder.Builder doubleValueRecorderBuilder(String name) {
    Objects.requireNonNull(name, "name");
    Utils.checkArgument(StringUtils.isValidMetricName(name), ERROR_MESSAGE_INVALID_NAME);
    return new NoopDoubleValueRecorder.NoopBuilder();
  }

  @Override
  public LongValueRecorder.Builder longValueRecorderBuilder(String name) {
    Objects.requireNonNull(name, "name");
    Utils.checkArgument(StringUtils.isValidMetricName(name), ERROR_MESSAGE_INVALID_NAME);
    return new NoopLongValueRecorder.NoopBuilder();
  }

  @Override
  public DoubleSumObserver.Builder doubleSumObserverBuilder(String name) {
    Objects.requireNonNull(name, "name");
    Utils.checkArgument(StringUtils.isValidMetricName(name), ERROR_MESSAGE_INVALID_NAME);
    return new NoopDoubleSumObserver.NoopBuilder();
  }

  @Override
  public LongSumObserver.Builder longSumObserverBuilder(String name) {
    Objects.requireNonNull(name, "name");
    Utils.checkArgument(StringUtils.isValidMetricName(name), ERROR_MESSAGE_INVALID_NAME);
    return new NoopLongSumObserver.NoopBuilder();
  }

  @Override
  public DoubleUpDownSumObserver.Builder doubleUpDownSumObserverBuilder(String name) {
    Objects.requireNonNull(name, "name");
    Utils.checkArgument(StringUtils.isValidMetricName(name), ERROR_MESSAGE_INVALID_NAME);
    return new NoopDoubleUpDownSumObserver.NoopBuilder();
  }

  @Override
  public LongUpDownSumObserver.Builder longUpDownSumObserverBuilder(String name) {
    Objects.requireNonNull(name, "name");
    Utils.checkArgument(StringUtils.isValidMetricName(name), ERROR_MESSAGE_INVALID_NAME);
    return new NoopLongUpDownSumObserver.NoopBuilder();
  }

  @Override
  public DoubleValueObserver.Builder doubleValueObserverBuilder(String name) {
    Objects.requireNonNull(name, "name");
    Utils.checkArgument(StringUtils.isValidMetricName(name), ERROR_MESSAGE_INVALID_NAME);
    return new NoopDoubleValueObserver.NoopBuilder();
  }

  @Override
  public LongValueObserver.Builder longValueObserverBuilder(String name) {
    Objects.requireNonNull(name, "name");
    Utils.checkArgument(StringUtils.isValidMetricName(name), ERROR_MESSAGE_INVALID_NAME);
    return new NoopLongValueObserver.NoopBuilder();
  }

  @Override
  public BatchRecorder newBatchRecorder(String... keyValuePairs) {
    Utils.validateLabelPairs(keyValuePairs);
    return NoopBatchRecorder.INSTANCE;
  }

  private DefaultMeter() {}

  /** No-op implementation of {@link DoubleCounter} interface. */
  @Immutable
  private static final class NoopDoubleCounter implements DoubleCounter {

    private NoopDoubleCounter() {}

    @Override
    public void add(double increment, Labels labels) {
      Objects.requireNonNull(labels, "labels");
      Utils.checkArgument(increment >= 0.0, COUNTERS_CAN_ONLY_INCREASE);
    }

    @Override
    public void add(double increment) {
      add(increment, Labels.empty());
    }

    @Override
    public NoopBoundDoubleCounter bind(Labels labels) {
      Objects.requireNonNull(labels, "labels");
      return NoopBoundDoubleCounter.INSTANCE;
    }

    @Immutable
    private enum NoopBoundDoubleCounter implements BoundDoubleCounter {
      INSTANCE;

      @Override
      public void add(double increment) {
        Utils.checkArgument(increment >= 0.0, COUNTERS_CAN_ONLY_INCREASE);
      }

      @Override
      public void unbind() {}
    }

    private static final class NoopBuilder extends NoopAbstractInstrumentBuilder<NoopBuilder>
        implements Builder {

      @Override
      protected NoopBuilder getThis() {
        return this;
      }

      @Override
      public DoubleCounter build() {
        return new NoopDoubleCounter();
      }
    }
  }

  /** No-op implementation of {@link LongCounter} interface. */
  @Immutable
  private static final class NoopLongCounter implements LongCounter {

    private NoopLongCounter() {}

    @Override
    public void add(long increment, Labels labels) {
      Objects.requireNonNull(labels, "labels");
      Utils.checkArgument(increment >= 0, COUNTERS_CAN_ONLY_INCREASE);
    }

    @Override
    public void add(long increment) {
      add(increment, Labels.empty());
    }

    @Override
    public NoopBoundLongCounter bind(Labels labels) {
      Objects.requireNonNull(labels, "labels");
      return NoopBoundLongCounter.INSTANCE;
    }

    @Immutable
    private enum NoopBoundLongCounter implements BoundLongCounter {
      INSTANCE;

      @Override
      public void add(long increment) {
        Utils.checkArgument(increment >= 0, COUNTERS_CAN_ONLY_INCREASE);
      }

      @Override
      public void unbind() {}
    }

    private static final class NoopBuilder extends NoopAbstractInstrumentBuilder<NoopBuilder>
        implements Builder {

      @Override
      protected NoopBuilder getThis() {
        return this;
      }

      @Override
      public LongCounter build() {
        return new NoopLongCounter();
      }
    }
  }

  /** No-op implementation of {@link DoubleUpDownCounter} interface. */
  @Immutable
  private static final class NoopDoubleUpDownCounter implements DoubleUpDownCounter {

    private NoopDoubleUpDownCounter() {}

    @Override
    public void add(double increment, Labels labels) {
      Objects.requireNonNull(labels, "labels");
    }

    @Override
    public void add(double increment) {
      add(increment, Labels.empty());
    }

    @Override
    public NoopBoundDoubleUpDownCounter bind(Labels labels) {
      Objects.requireNonNull(labels, "labels");
      return NoopBoundDoubleUpDownCounter.INSTANCE;
    }

    @Immutable
    private enum NoopBoundDoubleUpDownCounter implements BoundDoubleUpDownCounter {
      INSTANCE;

      @Override
      public void add(double increment) {}

      @Override
      public void unbind() {}
    }

    private static final class NoopBuilder extends NoopAbstractInstrumentBuilder<NoopBuilder>
        implements Builder {

      @Override
      protected NoopBuilder getThis() {
        return this;
      }

      @Override
      public DoubleUpDownCounter build() {
        return new NoopDoubleUpDownCounter();
      }
    }
  }

  /** No-op implementation of {@link LongUpDownCounter} interface. */
  @Immutable
  private static final class NoopLongUpDownCounter implements LongUpDownCounter {

    private NoopLongUpDownCounter() {}

    @Override
    public void add(long increment, Labels labels) {
      Objects.requireNonNull(labels, "labels");
    }

    @Override
    public void add(long increment) {
      add(increment, Labels.empty());
    }

    @Override
    public NoopBoundLongUpDownCounter bind(Labels labels) {
      Objects.requireNonNull(labels, "labels");
      return NoopBoundLongUpDownCounter.INSTANCE;
    }

    @Immutable
    private enum NoopBoundLongUpDownCounter implements BoundLongUpDownCounter {
      INSTANCE;

      @Override
      public void add(long increment) {}

      @Override
      public void unbind() {}
    }

    private static final class NoopBuilder extends NoopAbstractInstrumentBuilder<NoopBuilder>
        implements Builder {

      @Override
      protected NoopBuilder getThis() {
        return this;
      }

      @Override
      public LongUpDownCounter build() {
        return new NoopLongUpDownCounter();
      }
    }
  }

  /** No-op implementation of {@link DoubleValueRecorder} interface. */
  @Immutable
  private static final class NoopDoubleValueRecorder implements DoubleValueRecorder {

    private NoopDoubleValueRecorder() {}

    @Override
    public void record(double value, Labels labels) {
      Objects.requireNonNull(labels, "labels");
    }

    @Override
    public void record(double value) {
      record(value, Labels.empty());
    }

    @Override
    public NoopBoundDoubleValueRecorder bind(Labels labels) {
      Objects.requireNonNull(labels, "labels");
      return NoopBoundDoubleValueRecorder.INSTANCE;
    }

    @Immutable
    private enum NoopBoundDoubleValueRecorder implements BoundDoubleValueRecorder {
      INSTANCE;

      @Override
      public void record(double value) {}

      @Override
      public void unbind() {}
    }

    private static final class NoopBuilder extends NoopAbstractInstrumentBuilder<NoopBuilder>
        implements Builder {

      @Override
      protected NoopBuilder getThis() {
        return this;
      }

      @Override
      public DoubleValueRecorder build() {
        return new NoopDoubleValueRecorder();
      }
    }
  }

  /** No-op implementation of {@link LongValueRecorder} interface. */
  @Immutable
  private static final class NoopLongValueRecorder implements LongValueRecorder {

    private NoopLongValueRecorder() {}

    @Override
    public void record(long value, Labels labels) {
      Objects.requireNonNull(labels, "labels");
    }

    @Override
    public void record(long value) {
      record(value, Labels.empty());
    }

    @Override
    public NoopBoundLongValueRecorder bind(Labels labels) {
      Objects.requireNonNull(labels, "labels");
      return NoopBoundLongValueRecorder.INSTANCE;
    }

    @Immutable
    private enum NoopBoundLongValueRecorder implements BoundLongValueRecorder {
      INSTANCE;

      @Override
      public void record(long value) {}

      @Override
      public void unbind() {}
    }

    private static final class NoopBuilder extends NoopAbstractInstrumentBuilder<NoopBuilder>
        implements Builder {

      @Override
      protected NoopBuilder getThis() {
        return this;
      }

      @Override
      public LongValueRecorder build() {
        return new NoopLongValueRecorder();
      }
    }
  }

  /** No-op implementation of {@link DoubleSumObserver} interface. */
  @Immutable
  private static final class NoopDoubleSumObserver implements DoubleSumObserver {

    private NoopDoubleSumObserver() {}

    @Override
    public void setCallback(Callback<DoubleResult> callback) {
      Objects.requireNonNull(callback, "callback");
    }

    private static final class NoopBuilder extends NoopAbstractInstrumentBuilder<NoopBuilder>
        implements Builder {

      @Override
      protected NoopBuilder getThis() {
        return this;
      }

      @Override
      public DoubleSumObserver build() {
        return new NoopDoubleSumObserver();
      }
    }
  }

  /** No-op implementation of {@link LongSumObserver} interface. */
  @Immutable
  private static final class NoopLongSumObserver implements LongSumObserver {

    private NoopLongSumObserver() {}

    @Override
    public void setCallback(Callback<LongResult> callback) {
      Objects.requireNonNull(callback, "callback");
    }

    private static final class NoopBuilder extends NoopAbstractInstrumentBuilder<NoopBuilder>
        implements Builder {

      @Override
      protected NoopBuilder getThis() {
        return this;
      }

      @Override
      public LongSumObserver build() {
        return new NoopLongSumObserver();
      }
    }
  }

  /** No-op implementation of {@link DoubleUpDownSumObserver} interface. */
  @Immutable
  private static final class NoopDoubleUpDownSumObserver implements DoubleUpDownSumObserver {

    private NoopDoubleUpDownSumObserver() {}

    @Override
    public void setCallback(Callback<DoubleResult> callback) {
      Objects.requireNonNull(callback, "callback");
    }

    private static final class NoopBuilder extends NoopAbstractInstrumentBuilder<NoopBuilder>
        implements Builder {

      @Override
      protected NoopBuilder getThis() {
        return this;
      }

      @Override
      public DoubleUpDownSumObserver build() {
        return new NoopDoubleUpDownSumObserver();
      }
    }
  }

  /** No-op implementation of {@link LongUpDownSumObserver} interface. */
  @Immutable
  private static final class NoopLongUpDownSumObserver implements LongUpDownSumObserver {

    private NoopLongUpDownSumObserver() {}

    @Override
    public void setCallback(Callback<LongResult> callback) {
      Objects.requireNonNull(callback, "callback");
    }

    private static final class NoopBuilder extends NoopAbstractInstrumentBuilder<NoopBuilder>
        implements Builder {

      @Override
      protected NoopBuilder getThis() {
        return this;
      }

      @Override
      public LongUpDownSumObserver build() {
        return new NoopLongUpDownSumObserver();
      }
    }
  }

  /** No-op implementation of {@link DoubleValueObserver} interface. */
  @Immutable
  private static final class NoopDoubleValueObserver implements DoubleValueObserver {

    private NoopDoubleValueObserver() {}

    @Override
    public void setCallback(Callback<DoubleResult> callback) {
      Objects.requireNonNull(callback, "callback");
    }

    private static final class NoopBuilder extends NoopAbstractInstrumentBuilder<NoopBuilder>
        implements Builder {

      @Override
      protected NoopBuilder getThis() {
        return this;
      }

      @Override
      public DoubleValueObserver build() {
        return new NoopDoubleValueObserver();
      }
    }
  }

  /** No-op implementation of {@link LongValueObserver} interface. */
  @Immutable
  private static final class NoopLongValueObserver implements LongValueObserver {

    private NoopLongValueObserver() {}

    @Override
    public void setCallback(Callback<LongResult> callback) {
      Objects.requireNonNull(callback, "callback");
    }

    private static final class NoopBuilder extends NoopAbstractInstrumentBuilder<NoopBuilder>
        implements Builder {

      @Override
      protected NoopBuilder getThis() {
        return this;
      }

      @Override
      public LongValueObserver build() {
        return new NoopLongValueObserver();
      }
    }
  }

  /** No-op implementation of {@link BatchRecorder} interface. */
  private enum NoopBatchRecorder implements BatchRecorder {
    INSTANCE;

    @Override
    public BatchRecorder put(LongValueRecorder valueRecorder, long value) {
      Objects.requireNonNull(valueRecorder, "valueRecorder");
      return this;
    }

    @Override
    public BatchRecorder put(DoubleValueRecorder valueRecorder, double value) {
      Objects.requireNonNull(valueRecorder, "valueRecorder");
      return this;
    }

    @Override
    public BatchRecorder put(LongCounter counter, long value) {
      Objects.requireNonNull(counter, "counter");
      Utils.checkArgument(value >= 0, COUNTERS_CAN_ONLY_INCREASE);
      return this;
    }

    @Override
    public BatchRecorder put(DoubleCounter counter, double value) {
      Objects.requireNonNull(counter, "counter");
      Utils.checkArgument(value >= 0.0, COUNTERS_CAN_ONLY_INCREASE);
      return this;
    }

    @Override
    public BatchRecorder put(LongUpDownCounter upDownCounter, long value) {
      Objects.requireNonNull(upDownCounter, "upDownCounter");
      return this;
    }

    @Override
    public BatchRecorder put(DoubleUpDownCounter upDownCounter, double value) {
      Objects.requireNonNull(upDownCounter, "upDownCounter");
      return this;
    }

    @Override
    public void record() {}
  }

  private abstract static class NoopAbstractInstrumentBuilder<
          B extends NoopAbstractInstrumentBuilder<B>>
      implements Instrument.Builder {

    @Override
    public B setDescription(String description) {
      Objects.requireNonNull(description, "description");
      return getThis();
    }

    @Override
    public B setUnit(String unit) {
      Objects.requireNonNull(unit, "unit");
      return getThis();
    }

    protected abstract B getThis();
  }
}
