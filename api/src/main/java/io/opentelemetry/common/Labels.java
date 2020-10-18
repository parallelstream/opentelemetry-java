/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.common;

import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.concurrent.Immutable;

/** An immutable container for labels, which are pairs of {@link String}. */
@Immutable
public abstract class Labels extends ImmutableKeyValuePairs<String, String> {

  private static final Labels EMPTY = Labels.builder().build();

  public abstract void forEach(LabelConsumer consumer);

  @AutoValue
  @Immutable
  abstract static class ArrayBackedLabels extends Labels {
    ArrayBackedLabels() {}

    @Override
    abstract List<Object> data();

    @Override
    public void forEach(LabelConsumer consumer) {
      List<Object> data = data();
      for (int i = 0; i < data.size(); i += 2) {
        consumer.consume((String) data.get(i), (String) data.get(i + 1));
      }
    }
  }

  /** Returns a {@link Labels} instance with no attributes. */
  public static Labels empty() {
    return EMPTY;
  }

  /** Returns a {@link Labels} instance with a single key-value pair. */
  public static Labels of(String key, String value) {
    return sortAndFilterToLabels(key, value);
  }

  /**
   * Returns a {@link Labels} instance with two key-value pairs. Order of the keys is not preserved.
   * Duplicate keys will be removed.
   */
  public static Labels of(String key1, String value1, String key2, String value2) {
    return sortAndFilterToLabels(key1, value1, key2, value2);
  }

  /**
   * Returns a {@link Labels} instance with three key-value pairs. Order of the keys is not
   * preserved. Duplicate keys will be removed.
   */
  public static Labels of(
      String key1, String value1, String key2, String value2, String key3, String value3) {
    return sortAndFilterToLabels(key1, value1, key2, value2, key3, value3);
  }

  /**
   * Returns a {@link Labels} instance with four key-value pairs. Order of the keys is not
   * preserved. Duplicate keys will be removed.
   */
  public static Labels of(
      String key1,
      String value1,
      String key2,
      String value2,
      String key3,
      String value3,
      String key4,
      String value4) {
    return sortAndFilterToLabels(key1, value1, key2, value2, key3, value3, key4, value4);
  }

  /**
   * Returns a {@link Labels} instance with five key-value pairs. Order of the keys is not
   * preserved. Duplicate keys will be removed.
   */
  public static Labels of(
      String key1,
      String value1,
      String key2,
      String value2,
      String key3,
      String value3,
      String key4,
      String value4,
      String key5,
      String value5) {
    return sortAndFilterToLabels(
        key1, value1,
        key2, value2,
        key3, value3,
        key4, value4,
        key5, value5);
  }

  public static Labels of(String[] keyValueLabelPairs) {
    return sortAndFilterToLabels((Object[]) keyValueLabelPairs);
  }

  private static Labels sortAndFilterToLabels(Object... data) {
    return new AutoValue_Labels_ArrayBackedLabels(sortAndFilter(data));
  }

  /** Create a {@link Builder} pre-populated with the contents of this Labels instance. */
  public Builder toBuilder() {
    Builder builder = new Builder();
    builder.data.addAll(data());
    return builder;
  }

  /** Creates a new {@link Builder} instance for creating arbitrary {@link Labels}. */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Enables the creation of an {@link Labels} instance with an arbitrary number of key-value pairs.
   */
  public static class Builder {
    private final List<Object> data = new ArrayList<>();

    /** Create the {@link Labels} from this. */
    public Labels build() {
      return sortAndFilterToLabels(data.toArray());
    }

    /**
     * Sets a single label into this Builder.
     *
     * @return this Builder
     */
    public Builder setLabel(String key, String value) {
      data.add(key);
      data.add(value);
      return this;
    }
  }
}
