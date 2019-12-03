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
import com.ververica.statefun.flink.io.spi.SinkProvider;
import com.ververica.statefun.sdk.io.EgressIdentifier;
import com.ververica.statefun.sdk.io.EgressSpec;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

final class EgressToSinkTranslator {
  private final StatefulFunctionsUniverse universe;

  EgressToSinkTranslator(StatefulFunctionsUniverse universe) {
    this.universe = Objects.requireNonNull(universe);
  }

  Map<EgressIdentifier<?>, DecoratedSink> translate() {
    return Maps.transformValues(universe.egress(), this::sinkFromSpec);
  }

  private DecoratedSink sinkFromSpec(EgressSpec<?> spec) {
    SinkProvider provider = universe.sinks().get(spec.type());
    SinkFunction<?> sink = provider.forSpec(spec);

    return DecoratedSink.of(spec, sink);
  }
}
