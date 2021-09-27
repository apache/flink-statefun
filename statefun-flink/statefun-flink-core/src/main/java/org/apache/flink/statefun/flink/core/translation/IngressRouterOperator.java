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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverse;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverses;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.flink.core.message.MessageFactory;
import org.apache.flink.statefun.flink.core.message.MessageFactoryKey;
import org.apache.flink.statefun.flink.core.metrics.FlinkUserMetrics;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.Router;
import org.apache.flink.statefun.sdk.io.Router.Downstream;
import org.apache.flink.statefun.sdk.metrics.Metrics;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class IngressRouterOperator<T> extends AbstractStreamOperator<Message>
    implements OneInputStreamOperator<T, Message> {

  private static final Logger LOG = LoggerFactory.getLogger(IngressRouterOperator.class);

  private static final long serialVersionUID = 1;

  private final StatefulFunctionsConfig configuration;
  private final IngressIdentifier<T> id;
  private transient List<Router<T>> routers;
  private transient DownstreamCollector<T> downstream;

  IngressRouterOperator(StatefulFunctionsConfig configuration, IngressIdentifier<T> id) {
    this.configuration = configuration;
    this.id = Objects.requireNonNull(id);
    this.chainingStrategy = ChainingStrategy.ALWAYS;
  }

  @Override
  public void open() throws Exception {
    super.open();

    StatefulFunctionsUniverse universe =
        StatefulFunctionsUniverses.get(
            Thread.currentThread().getContextClassLoader(), configuration);

    LOG.info("Using message factory key " + universe.messageFactoryKey());

    MetricGroup routersMetricGroup =
        getMetricGroup().addGroup("routers").addGroup(id.namespace()).addGroup(id.name());
    Metrics routersMetrics = new FlinkUserMetrics(routersMetricGroup);

    this.routers = loadRoutersAttachedToIngress(id, universe.routers());
    this.downstream =
        new DownstreamCollector<>(universe.messageFactoryKey(), output, routersMetrics);
  }

  @Override
  public void processElement(StreamRecord<T> element) {
    final T value = element.getValue();
    for (Router<T> router : routers) {
      router.route(value, downstream);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> List<Router<T>> loadRoutersAttachedToIngress(
      IngressIdentifier<T> id, Map<IngressIdentifier<?>, List<Router<?>>> definedRouters) {

    List<Router<?>> routerList = definedRouters.get(id);
    Preconditions.checkState(routerList != null, "unable to find a router for ingress " + id);
    return (List<Router<T>>) (List<?>) routerList;
  }

  @VisibleForTesting
  static final class DownstreamCollector<T> implements Downstream<T> {

    private final MessageFactory factory;
    private final StreamRecord<Message> reuse = new StreamRecord<>(null);
    private final Output<StreamRecord<Message>> output;
    private final Metrics metrics;

    DownstreamCollector(
        MessageFactoryKey messageFactoryKey,
        Output<StreamRecord<Message>> output,
        Metrics metrics) {
      this.factory = MessageFactory.forKey(messageFactoryKey);
      this.output = Objects.requireNonNull(output);
      this.metrics = Objects.requireNonNull(metrics);
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
      Message envelope = factory.from(null, to, message);
      output.collect(reuse.replace(envelope));
    }

    @Override
    public Metrics metrics() {
      return metrics;
    }
  }
}
