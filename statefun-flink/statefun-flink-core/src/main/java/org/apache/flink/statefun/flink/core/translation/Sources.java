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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.statefun.flink.common.UnimplementedTypeInfo;
import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverse;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.message.RoutableMessage;
import org.apache.flink.statefun.flink.core.types.StaticallyRegisteredTypes;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
        sourceFunctionToDataStream(env, sourceFunctions);

    final Map<IngressIdentifier<?>, DataStream<Message>> envelopeSources =
        dataStreamToEnvelopStream(universe, sourceStreams, configuration);

    return new Sources(union(envelopeSources.values()));
  }

  static Sources create(
      StaticallyRegisteredTypes types, Iterable<DataStream<RoutableMessage>> envelopeSources) {
    TypeInformation<Message> messageOutputType = types.registerType(Message.class);

    List<DataStream<Message>> messages = new ArrayList<>();

    for (DataStream<? extends RoutableMessage> input : envelopeSources) {
      /* This is safe, since the SDK is producing Messages. */
      @SuppressWarnings("unchecked")
      DataStream<Message> casted = (DataStream<Message>) input;
      casted.getTransformation().setOutputType(messageOutputType);
      messages.add(casted);
    }

    return new Sources(union(messages));
  }

  private static Map<IngressIdentifier<?>, DataStream<Message>> dataStreamToEnvelopStream(
      StatefulFunctionsUniverse universe,
      Map<IngressIdentifier<?>, DataStream<?>> sourceStreams,
      StatefulFunctionsConfig configuration) {

    RouterTranslator routerTranslator = new RouterTranslator(universe, configuration);
    return routerTranslator.translate(sourceStreams);
  }

  private static Map<IngressIdentifier<?>, DataStream<?>> sourceFunctionToDataStream(
      StreamExecutionEnvironment env, Map<IngressIdentifier<?>, DecoratedSource> sourceFunctions) {

    Map<IngressIdentifier<?>, DataStream<?>> sourceStreams = new HashMap<>();
    sourceFunctions.forEach(
        (id, sourceFunction) -> {
          DataStreamSource<?> stream = env.addSource(sourceFunction.source);

          stream.name(sourceFunction.name);
          stream.uid(sourceFunction.uid);

          // we erase whatever type information present at the source, since the source is always
          // chained to the IngressRouterFlatMap, and that operator is always emitting records of
          // type
          // Message.
          eraseTypeInformation(stream.getTransformation());
          sourceStreams.put(id, stream);
        });
    return sourceStreams;
  }

  private static void eraseTypeInformation(Transformation<?> transformation) {
    transformation.setOutputType(new UnimplementedTypeInfo<>());
  }

  private static Map<IngressIdentifier<?>, DecoratedSource> ingressToSourceFunction(
      StatefulFunctionsUniverse universe) {
    IngressToSourceFunctionTranslator translator = new IngressToSourceFunctionTranslator(universe);
    return translator.translate();
  }

  DataStream<Message> unionStream() {
    return sourceUnion;
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
}
