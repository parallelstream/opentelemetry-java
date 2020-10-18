/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.trace.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.opentelemetry.sdk.trace.Sampler;
import io.opentelemetry.sdk.trace.Samplers;
import org.junit.jupiter.api.Test;

class TraceConfigTest {

  @Test
  void defaultTraceConfig() {
    assertThat(TraceConfig.getDefault().getSampler().getDescription())
        .isEqualTo(Samplers.parentBased(Samplers.alwaysOn()).getDescription());
    assertThat(TraceConfig.getDefault().getMaxNumberOfAttributes()).isEqualTo(1000);
    assertThat(TraceConfig.getDefault().getMaxNumberOfEvents()).isEqualTo(1000);
    assertThat(TraceConfig.getDefault().getMaxNumberOfLinks()).isEqualTo(1000);
    assertThat(TraceConfig.getDefault().getMaxNumberOfAttributesPerEvent()).isEqualTo(32);
    assertThat(TraceConfig.getDefault().getMaxNumberOfAttributesPerLink()).isEqualTo(32);
  }

  @Test
  void updateTraceConfig_NullSampler() {
    assertThrows(
        NullPointerException.class,
        () -> TraceConfig.getDefault().toBuilder().setSampler(null).build());
  }

  @Test
  void updateTraceConfig_NonPositiveMaxNumberOfAttributes() {
    assertThrows(
        IllegalArgumentException.class,
        () -> TraceConfig.getDefault().toBuilder().setMaxNumberOfAttributes(0).build());
  }

  @Test
  void updateTraceConfig_NonPositiveMaxNumberOfEvents() {
    assertThrows(
        IllegalArgumentException.class,
        () -> TraceConfig.getDefault().toBuilder().setMaxNumberOfEvents(0).build());
  }

  @Test
  void updateTraceConfig_NonPositiveMaxNumberOfLinks() {
    assertThrows(
        IllegalArgumentException.class,
        () -> TraceConfig.getDefault().toBuilder().setMaxNumberOfLinks(0).build());
  }

  @Test
  void updateTraceConfig_NonPositiveMaxNumberOfAttributesPerEvent() {
    assertThrows(
        IllegalArgumentException.class,
        () -> TraceConfig.getDefault().toBuilder().setMaxNumberOfAttributesPerEvent(0).build());
  }

  @Test
  void updateTraceConfig_NonPositiveMaxNumberOfAttributesPerLink() {
    assertThrows(
        IllegalArgumentException.class,
        () -> TraceConfig.getDefault().toBuilder().setMaxNumberOfAttributesPerLink(0).build());
  }

  @Test
  void updateTraceConfig_InvalidTraceIdRatioBased() {
    assertThrows(
        IllegalArgumentException.class,
        () -> TraceConfig.getDefault().toBuilder().setTraceIdRatioBased(2).build());
  }

  @Test
  void updateTraceConfig_NegativeTraceIdRatioBased() {
    assertThrows(
        IllegalArgumentException.class,
        () -> TraceConfig.getDefault().toBuilder().setTraceIdRatioBased(-1).build());
  }

  @Test
  void updateTraceConfig_OffTraceIdRatioBased() {
    TraceConfig traceConfig = TraceConfig.getDefault().toBuilder().setTraceIdRatioBased(0).build();
    assertThat(traceConfig.getSampler()).isSameAs(Samplers.alwaysOff());
  }

  @Test
  void updateTraceConfig_OnTraceIdRatioBased() {
    TraceConfig traceConfig = TraceConfig.getDefault().toBuilder().setTraceIdRatioBased(1).build();

    Sampler sampler = traceConfig.getSampler();
    assertThat(sampler).isEqualTo(Samplers.parentBased(Samplers.alwaysOn()));
  }

  @Test
  void updateTraceConfig_All() {
    TraceConfig traceConfig =
        TraceConfig.getDefault()
            .toBuilder()
            .setSampler(Samplers.alwaysOff())
            .setMaxNumberOfAttributes(8)
            .setMaxNumberOfEvents(10)
            .setMaxNumberOfLinks(11)
            .setMaxNumberOfAttributesPerEvent(1)
            .setMaxNumberOfAttributesPerLink(2)
            .build();
    assertThat(traceConfig.getSampler()).isEqualTo(Samplers.alwaysOff());
    assertThat(traceConfig.getMaxNumberOfAttributes()).isEqualTo(8);
    assertThat(traceConfig.getMaxNumberOfEvents()).isEqualTo(10);
    assertThat(traceConfig.getMaxNumberOfLinks()).isEqualTo(11);
    assertThat(traceConfig.getMaxNumberOfAttributesPerEvent()).isEqualTo(1);
    assertThat(traceConfig.getMaxNumberOfAttributesPerLink()).isEqualTo(2);
  }
}
