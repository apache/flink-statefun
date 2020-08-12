/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.statefun.flink.core.translation;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.flink.statefun.flink.common.UnimplementedTypeInfo;
import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverse;
import org.apache.flink.statefun.flink.core.classloader.ModuleAwareUdfStreamOperatorFactory;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;

final class Sources {

  private final DataStream<Message> sourceUnion;

  private Sources(DataStream<Message> union) {
    this.sourceUnion = union;
  }

  static Sources create(
      StreamExecutionEnvironment env,
      StatefulFunctionsUniverse universe,
      StatefulFunctionsConfig configuration) {
    final Map<IngressIdentifier<?>, DecoratedSource> sourceFunctions =
        ingressToSourceFunction(universe);

    final Map<IngressIdentifier<?>, DataStream<?>> sourceStreams =
        sourceFunctionToDataStream(env, configuration, sourceFunctions);

    final Map<IngressIdentifier<?>, DataStream<Message>> envelopeSources =
        dataStreamToEnvelopStream(universe, sourceStreams, configuration);

    return new Sources(union(envelopeSources.values()));
  }

  private static Map<IngressIdentifier<?>, DataStream<Message>> dataStreamToEnvelopStream(
      StatefulFunctionsUniverse universe,
      Map<IngressIdentifier<?>, DataStream<?>> sourceStreams,
      StatefulFunctionsConfig configuration) {

    RouterTranslator routerTranslator = new RouterTranslator(universe, configuration);
    return routerTranslator.translate(sourceStreams);
  }

  private static Map<IngressIdentifier<?>, DataStream<?>> sourceFunctionToDataStream(
      StreamExecutionEnvironment env,
      StatefulFunctionsConfig configuration,
      Map<IngressIdentifier<?>, DecoratedSource> sourceFunctions) {

    Map<IngressIdentifier<?>, DataStream<?>> sourceStreams = new HashMap<>();
    sourceFunctions.forEach(
        (id, source) -> {
          SourceFunction<?> function = env.clean(source.source);
          StreamSource<?, ?> operator = new StreamSource<>(function);

          // we erase whatever type information present at the source, since the source is always
          // chained to the IngressRouter, and that operator is always emitting records of
          // type Message.
          LegacySourceTransformation<?> transformation =
              new LegacySourceTransformation<>(
                  source.name,
                  ModuleAwareUdfStreamOperatorFactory.of(configuration, operator),
                  new UnimplementedTypeInfo<>(),
                  function instanceof ParallelSourceFunction ? env.getParallelism() : 1);

          StatefulFunctionSingleOuputStreamOperator<?> stream =
              new StatefulFunctionSingleOuputStreamOperator<>(env, transformation);
          stream.uid(source.uid);

          sourceStreams.put(id, stream);
        });
    return sourceStreams;
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
