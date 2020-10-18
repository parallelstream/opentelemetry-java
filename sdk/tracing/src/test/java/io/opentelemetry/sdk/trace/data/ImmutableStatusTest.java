/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.trace.data;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.sdk.trace.data.SpanData.Status;
import io.opentelemetry.trace.StatusCode;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link ImmutableStatus}. */
class ImmutableStatusTest {
  @Test
  void defaultConstants() {
    StatusCode[] codes = StatusCode.values();
    assertThat(codes).hasSize(3);
    assertThat(Status.unset().getCanonicalCode()).isEqualTo(StatusCode.UNSET);
    assertThat(Status.unset().getDescription()).isNull();
    assertThat(Status.ok().getCanonicalCode()).isEqualTo(StatusCode.OK);
    assertThat(Status.ok().getDescription()).isNull();
    assertThat(Status.error().getCanonicalCode()).isEqualTo(StatusCode.ERROR);
    assertThat(Status.error().getDescription()).isNull();
  }

  @Test
  void generateCodeToStatus() {
    StatusCode[] codes = StatusCode.values();
    assertThat(ImmutableStatus.codeToStatus).hasSize(codes.length);
    for (StatusCode code : codes) {
      assertThat(ImmutableStatus.codeToStatus.get(code)).isNotNull();
      assertThat(ImmutableStatus.codeToStatus.get(code).getCanonicalCode()).isEqualTo(code);
      assertThat(ImmutableStatus.codeToStatus.get(code).getDescription()).isNull();
    }
  }
}
