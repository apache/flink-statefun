package org.apache.flink.statefun.flink.core.jsonmodule;

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.statefun.flink.io.spi.JsonEgressSpec;
import org.apache.flink.statefun.flink.io.spi.JsonIngressSpec;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.Router;

/** Parses a given module's JSON specification. */
abstract class JsonModuleSpecParser {

  protected final JsonNode spec;

  JsonModuleSpecParser(JsonNode spec) {
    this.spec = Objects.requireNonNull(spec);
  }

  protected abstract Map<FunctionSpec.Kind, Map<FunctionType, FunctionSpec>> parseFunctions();

  protected abstract Iterable<Tuple2<IngressIdentifier<Message>, Router<Message>>> parseRouters();

  protected abstract Iterable<JsonIngressSpec<Message>> parseIngressSpecs();

  protected abstract Iterable<JsonEgressSpec<Any>> parseEgressSpecs();
}
