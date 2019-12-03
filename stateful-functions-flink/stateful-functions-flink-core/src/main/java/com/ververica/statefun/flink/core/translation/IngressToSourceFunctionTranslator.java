/*
 * Copyright 2019 Ververica GmbH.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.statefun.flink.core.translation;

import com.ververica.statefun.flink.core.StatefulFunctionsUniverse;
import com.ververica.statefun.flink.core.common.Maps;
import com.ververica.statefun.flink.io.spi.SourceProvider;
import com.ververica.statefun.sdk.io.IngressIdentifier;
import com.ververica.statefun.sdk.io.IngressSpec;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

final class IngressToSourceFunctionTranslator {
  private final StatefulFunctionsUniverse universe;

  IngressToSourceFunctionTranslator(StatefulFunctionsUniverse universe) {
    this.universe = Objects.requireNonNull(universe);
  }

  Map<IngressIdentifier<?>, DecoratedSource> translate() {
    return Maps.transformValues(universe.ingress(), this::sourceFromSpec);
  }

  private DecoratedSource sourceFromSpec(IngressIdentifier<?> key, IngressSpec<?> spec) {
    SourceProvider provider = universe.sources().get(spec.type());
    if (provider == null) {
      throw new IllegalStateException(
          "Unable to find a source translation for ingress of type "
              + spec.type()
              + ", which is bound for key "
              + key);
    }
    SourceFunction<?> source = provider.forSpec(spec);
    if (source == null) {
      throw new NullPointerException(
          "A source provider for type " + spec.type() + ", has produced a NULL source.");
    }
    return DecoratedSource.of(spec, source);
  }
}
