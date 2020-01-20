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

import com.google.protobuf.Any;
import com.ververica.statefun.flink.core.StatefulFunctionsUniverse;
import com.ververica.statefun.flink.core.StatefulFunctionsUniverses;
import com.ververica.statefun.flink.core.message.Message;
import com.ververica.statefun.flink.core.message.MessageFactory;
import com.ververica.statefun.flink.core.message.MessageFactoryType;
import com.ververica.statefun.sdk.Address;
import com.ververica.statefun.sdk.io.IngressIdentifier;
import com.ververica.statefun.sdk.io.Router;
import com.ververica.statefun.sdk.io.Router.Downstream;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.api.common.ExecutionConfig.GlobalJobParameters;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

public final class IngressRouterFlatMap<T> extends RichFlatMapFunction<T, Message> {

  private static final long serialVersionUID = 1;

  private final IngressIdentifier<T> id;
  private transient List<Router<T>> routers;
  private transient DownstreamCollector<T> downstream;

  IngressRouterFlatMap(IngressIdentifier<T> id) {
    this.id = Objects.requireNonNull(id);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    Configuration configuration = combineWithGlobalJobConfiguration(parameters);
    StatefulFunctionsUniverse universe =
        StatefulFunctionsUniverses.get(
            Thread.currentThread().getContextClassLoader(), configuration);

    this.downstream = new DownstreamCollector<>(universe.messageFactoryType());
    this.routers = loadRoutersAttachedToIngress(id, universe.routers());
  }

  @Override
  public void flatMap(T in, Collector<Message> collector) {
    downstream.collector = collector;
    for (Router<T> router : routers) {
      router.route(in, downstream);
    }
  }

  private Configuration combineWithGlobalJobConfiguration(Configuration parameters) {
    Configuration combined = new Configuration();
    combined.addAll(parameters);

    GlobalJobParameters globalJobParameters =
        getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

    Preconditions.checkState(globalJobParameters instanceof Configuration);
    Configuration configuration = (Configuration) globalJobParameters;

    combined.addAll(configuration);
    return combined;
  }

  @SuppressWarnings("unchecked")
  private static <T> List<Router<T>> loadRoutersAttachedToIngress(
      IngressIdentifier<T> id, Map<IngressIdentifier<?>, List<Router<?>>> definedRouters) {

    List<Router<?>> routerList = definedRouters.get(id);
    Preconditions.checkState(routerList != null, "unable to find a router for ingress " + id);
    return (List<Router<T>>) (List<?>) routerList;
  }

  private static final class DownstreamCollector<T> implements Downstream<T> {

    private final MessageFactory factory;
    private final boolean multiLanguagePayloads;

    Collector<Message> collector;

    DownstreamCollector(MessageFactoryType messageFactoryType) {
      this.factory = MessageFactory.forType(messageFactoryType);
      this.multiLanguagePayloads =
          messageFactoryType == MessageFactoryType.WITH_PROTOBUF_PAYLOADS_MULTILANG;
    }

    @Override
    public void forward(Address to, Object message) {
      if (to == null) {
        throw new NullPointerException("Unable to send a message downstream without an address.");
      }
      if (message == null) {
        throw new NullPointerException("message is mandatory parameter and can not be NULL.");
      }
      // create an envelope out of the source, destination addresses, and the payload.
      // This is the first instance where a user supplied payload that comes off a source
      // is wrapped into a (statefun) Message envelope.
      if (multiLanguagePayloads) {
        // in the multi language case, the payloads are always of type com.google.protobuf.Any
        // therefore we first pack the whatever protobuf Message supplied from the user
        // into a Protobuf Any.
        message = wrapAsProtobufAny(message);
      }
      Message envelope = factory.from(null, to, message);
      collector.collect(envelope);
    }

    private Object wrapAsProtobufAny(Object message) {
      if (message instanceof Any) {
        return message;
      }
      return Any.pack((com.google.protobuf.Message) message);
    }
  }
}
