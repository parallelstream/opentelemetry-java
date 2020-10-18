/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.extensions.trace.propagation;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapPropagator;
import java.util.Optional;
import javax.annotation.concurrent.Immutable;

@Immutable
interface B3PropagatorExtractor {

  <C> Optional<Context> extract(Context context, C carrier, TextMapPropagator.Getter<C> getter);
}
