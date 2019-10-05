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
import com.ververica.statefun.flink.core.message.Message;
import com.ververica.statefun.flink.core.types.StaticallyRegisteredTypes;
import com.ververica.statefun.sdk.io.IngressIdentifier;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

final class Sources {

  private final DataStream<Message> sourceUnion;

  private Sources(DataStream<Message> union) {
    this.sourceUnion = union;
  }

  static Sources create(StreamExecutionEnvironment env, StatefulFunctionsUniverse universe) {
    final Map<IngressIdentifier<?>, DecoratedSource> sourceFunctions =
        ingressToSourceFunction(universe);

    final Map<IngressIdentifier<?>, DataStream<?>> sourceStreams =
        sourceFunctionToDataStream(env, universe, sourceFunctions);

    final Map<IngressIdentifier<?>, DataStream<Message>> envelopeSources =
        dataStreamToEnvelopStream(universe, sourceStreams);

    return new Sources(union(envelopeSources.values()));
  }

  private static Map<IngressIdentifier<?>, DataStream<Message>> dataStreamToEnvelopStream(
      StatefulFunctionsUniverse universe, Map<IngressIdentifier<?>, DataStream<?>> sourceStreams) {

    RouterTranslator routerTranslator = new RouterTranslator(universe);
    return routerTranslator.translate(sourceStreams);
  }

  private static Map<IngressIdentifier<?>, DataStream<?>> sourceFunctionToDataStream(
      StreamExecutionEnvironment env,
      StatefulFunctionsUniverse universe,
      Map<IngressIdentifier<?>, DecoratedSource> sourceFunctions) {

    Map<IngressIdentifier<?>, DataStream<?>> sourceStreams = new HashMap<>();
    sourceFunctions.forEach(
        (id, sourceFunction) -> {
          DataStreamSource<?> stream = env.addSource(sourceFunction.source);

          stream.name(sourceFunction.name);
          stream.uid(sourceFunction.uid);

          // we have to overwrite the TypeInformation of the produced type
          // because @sourceFunction might be of type ResultTypeQueryable,
          // but we need to use the type information that is derived from
          // the ingress identifier type().
          setOutputType(universe.types(), id.producedType(), stream.getTransformation());

          sourceStreams.put(id, stream);
        });
    return sourceStreams;
  }

  @SuppressWarnings({"unchecked", "raw"})
  private static void setOutputType(
      StaticallyRegisteredTypes types, Class<?> type, Transformation<?> transformation) {

    TypeInformation typeInfo = types.registerType(type);
    transformation.setOutputType(typeInfo);
  }

  private static Map<IngressIdentifier<?>, DecoratedSource> ingressToSourceFunction(
      StatefulFunctionsUniverse universe) {
    IngressToSourceFunctionTranslator translator = new IngressToSourceFunctionTranslator(universe);
    return translator.translate();
  }

  private static <T> DataStream<T> union(Collection<DataStream<T>> sources) {
    if (sources.isEmpty()) {
      throw new IllegalStateException("There are no routers defined.");
    }
    final int sourceCount = sources.size();
    final Iterator<DataStream<T>> iterator = sources.iterator();
    if (sourceCount == 1) {
      return iterator.next();
    }
    DataStream<T> first = iterator.next();
    @SuppressWarnings("unchecked")
    DataStream<T>[] rest = new DataStream[sourceCount - 1];
    for (int i = 0; i < sourceCount - 1; i++) {
      rest[i] = iterator.next();
    }
    return first.union(rest);
  }

  DataStream<Message> unionStream() {
    return sourceUnion;
  }
}
