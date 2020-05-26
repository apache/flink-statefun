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
package org.apache.flink.statefun.flink.core.jsonmodule;

import static java.lang.String.format;
import static org.apache.flink.statefun.flink.core.common.Maps.transformValues;

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import java.net.URL;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.statefun.flink.core.grpcfn.GrpcFunctionProvider;
import org.apache.flink.statefun.flink.core.grpcfn.GrpcFunctionSpec;
import org.apache.flink.statefun.flink.core.httpfn.HttpFunctionProvider;
import org.apache.flink.statefun.flink.core.httpfn.HttpFunctionSpec;
import org.apache.flink.statefun.flink.core.jsonmodule.FunctionSpec.Kind;
import org.apache.flink.statefun.flink.core.protorouter.AutoRoutableProtobufRouter;
import org.apache.flink.statefun.flink.io.kafka.ProtobufKafkaIngressTypes;
import org.apache.flink.statefun.flink.io.kinesis.PolyglotKinesisIOTypes;
import org.apache.flink.statefun.flink.io.spi.JsonEgressSpec;
import org.apache.flink.statefun.flink.io.spi.JsonIngressSpec;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.IngressType;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.Router;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

final class JsonModule implements StatefulFunctionModule {
  private final URL moduleUrl;

  private final JsonModuleSpecParser specParser;

  public JsonModule(JsonModuleSpecParser specParser, URL moduleUrl) {
    this.specParser = Objects.requireNonNull(specParser);
    this.moduleUrl = Objects.requireNonNull(moduleUrl);
  }

  public void configure(Map<String, String> conf, Binder binder) {
    try {
      configureFunctions(binder, specParser.parseFunctions());
      configureIngress(binder, specParser.parseIngressSpecs());
      configureRouters(binder, specParser.parseRouters());
      configureEgress(binder, specParser.parseEgressSpecs());
    } catch (Throwable t) {
      throw new ModuleConfigurationException(
          format("Error while parsing module at %s", moduleUrl), t);
    }
  }

  private static void configureFunctions(
      Binder binder, Map<Kind, Map<FunctionType, FunctionSpec>> functions) {
    for (Entry<Kind, Map<FunctionType, FunctionSpec>> entry : functions.entrySet()) {
      StatefulFunctionProvider provider = functionProvider(entry.getKey(), entry.getValue());
      Set<FunctionType> functionTypes = entry.getValue().keySet();
      for (FunctionType type : functionTypes) {
        binder.bindFunctionProvider(type, provider);
      }
    }
  }

  private static StatefulFunctionProvider functionProvider(
      Kind kind, Map<FunctionType, FunctionSpec> definedFunctions) {
    switch (kind) {
      case HTTP:
        return new HttpFunctionProvider(
            transformValues(definedFunctions, HttpFunctionSpec.class::cast));
      case GRPC:
        return new GrpcFunctionProvider(
            transformValues(definedFunctions, GrpcFunctionSpec.class::cast));
      default:
        throw new IllegalStateException("Unexpected value: " + kind);
    }
  }

  private static void configureRouters(
      Binder binder, Iterable<Tuple2<IngressIdentifier<Message>, Router<Message>>> routers) {
    routers.forEach(
        ingressIdAndRouter ->
            binder.bindIngressRouter(ingressIdAndRouter.f0, ingressIdAndRouter.f1));
  }

  private static void configureIngress(
      Binder binder, Iterable<JsonIngressSpec<Message>> ingressSpecs) {
    ingressSpecs.forEach(
        ingressSpec -> {
          binder.bindIngress(ingressSpec);

          if (isAutoRoutableIngress(ingressSpec)) {
            binder.bindIngressRouter(ingressSpec.id(), new AutoRoutableProtobufRouter());
          }
        });
  }

  private static void configureEgress(Binder binder, Iterable<JsonEgressSpec<Any>> egressSpecs) {
    egressSpecs.forEach(binder::bindEgress);
  }

  private static boolean isAutoRoutableIngress(JsonIngressSpec<Message> ingressSpec) {
    final IngressType ingressType = ingressSpec.type();

    return ingressType.equals(ProtobufKafkaIngressTypes.ROUTABLE_PROTOBUF_KAFKA_INGRESS_TYPE)
        || ingressType.equals(PolyglotKinesisIOTypes.ROUTABLE_PROTOBUF_KINESIS_INGRESS_TYPE);
  }
}
