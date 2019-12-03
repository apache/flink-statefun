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

import com.ververica.statefun.flink.core.StatefulFunctionsJobConstants;
import com.ververica.statefun.flink.core.StatefulFunctionsUniverse;
import com.ververica.statefun.flink.core.common.Maps;
import com.ververica.statefun.flink.core.message.Message;
import com.ververica.statefun.sdk.io.IngressIdentifier;
import com.ververica.statefun.sdk.io.IngressSpec;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;

final class RouterTranslator {
  private final StatefulFunctionsUniverse universe;

  RouterTranslator(StatefulFunctionsUniverse universe) {
    this.universe = universe;
  }

  Map<IngressIdentifier<?>, DataStream<Message>> translate(
      Map<IngressIdentifier<?>, DataStream<?>> sources) {
    return Maps.transformValues(
        universe.routers(), (id, unused) -> createRoutersForSource(id, sources.get(id)));
  }

  /**
   * For each input {@linkplain DataStream} (created as a result of {@linkplain IngressSpec}
   * translation) we attach a single FlatMap function that would invoke all the defined routers for
   * that spec. Please note that the FlatMap function must have the same parallelism as the
   * {@linkplain DataStream} it is attached to, so that we keep per key ordering.
   */
  @SuppressWarnings("unchecked")
  private DataStream<Message> createRoutersForSource(
      IngressIdentifier<?> id, DataStream<?> sourceStream) {
    IngressIdentifier<Object> castedId = (IngressIdentifier<Object>) id;
    DataStream<Object> castedSource = (DataStream<Object>) sourceStream;

    IngressRouterFlatMap<Object> router = new IngressRouterFlatMap<>(castedId);

    TypeInformation<Message> typeInfo = universe.types().registerType(Message.class);

    int sourceParallelism = castedSource.getParallelism();

    return castedSource
        .flatMap(router)
        .name(StatefulFunctionsJobConstants.ROUTER_NAME + " (" + castedId.name() + ")")
        .returns(typeInfo)
        .setParallelism(sourceParallelism);
  }
}
