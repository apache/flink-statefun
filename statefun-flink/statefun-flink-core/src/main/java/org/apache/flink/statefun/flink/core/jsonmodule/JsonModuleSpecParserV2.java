package org.apache.flink.statefun.flink.core.jsonmodule;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;

import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.statefun.flink.common.json.NamespaceNamePair;
import org.apache.flink.statefun.flink.common.json.Selectors;
import org.apache.flink.statefun.flink.core.grpcfn.GrpcFunctionSpec;
import org.apache.flink.statefun.flink.core.httpfn.HttpFunctionSpec;
import org.apache.flink.statefun.flink.core.httpfn.StateSpec;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.util.TimeUtils;

final class JsonModuleSpecParserV2 extends JsonModuleSpecParserV1 {

  JsonModuleSpecParserV2(JsonNode spec) {
    super(spec);
  }

  @Override
  protected Map<FunctionSpec.Kind, Map<FunctionType, FunctionSpec>> parseFunctions() {
    final Iterable<? extends JsonNode> functions =
        Selectors.listAt(spec, Pointers.FUNCTIONS_POINTER);

    return StreamSupport.stream(functions.spliterator(), false)
        .map(JsonModuleSpecParserV2::parseFunctionSpec)
        .collect(groupingBy(FunctionSpec::kind, groupByFunctionType()));
  }

  // ----------------------------------------------------------------------------------------------------------
  // Functions
  // ----------------------------------------------------------------------------------------------------------

  private static FunctionSpec parseFunctionSpec(JsonNode functionNode) {
    String functionKind = Selectors.textAt(functionNode, Pointers.Functions.META_KIND);
    FunctionSpec.Kind kind =
        FunctionSpec.Kind.valueOf(functionKind.toUpperCase(Locale.getDefault()));
    FunctionType functionType = functionType(functionNode);
    switch (kind) {
      case HTTP:
        final HttpFunctionSpec.Builder specBuilder =
            HttpFunctionSpec.builder(functionType, functionUri(functionNode));

        for (StateSpec state : functionStates(functionNode)) {
          specBuilder.withState(state);
        }
        optionalMaxRequestDuration(functionNode).ifPresent(specBuilder::withMaxRequestDuration);
        optionalMaxNumBatchRequests(functionNode).ifPresent(specBuilder::withMaxNumBatchRequests);

        return specBuilder.build();
      case GRPC:
        return new GrpcFunctionSpec(functionType, functionAddress(functionNode));
      default:
        throw new IllegalArgumentException("Unrecognized function kind " + functionKind);
    }
  }

  private static OptionalInt optionalMaxNumBatchRequests(JsonNode functionNode) {
    return Selectors.optionalIntegerAt(
        functionNode, Pointers.Functions.FUNCTION_MAX_NUM_BATCH_REQUESTS);
  }

  private static Optional<Duration> optionalMaxRequestDuration(JsonNode functionNode) {
    return Selectors.optionalTextAt(functionNode, Pointers.Functions.FUNCTION_TIMEOUT)
        .map(TimeUtils::parseDuration);
  }

  private static List<StateSpec> functionStates(JsonNode functionNode) {
    final Iterable<? extends JsonNode> stateSpecNodes =
        Selectors.listAt(functionNode, Pointers.Functions.FUNCTION_STATES);
    final List<StateSpec> parsedStateSpecs = new ArrayList<>();

    for (JsonNode stateSpecNode : stateSpecNodes) {
      final String stateName =
          Selectors.textAt(stateSpecNode, Pointers.FunctionStates.FUNCTION_STATE_NAME);
      final OptionalLong optionalStateTtl =
          Selectors.optionalLongAt(
              stateSpecNode, Pointers.FunctionStates.FUNCTION_STATE_TTL_DURATION);

      if (optionalStateTtl.isPresent()) {
        parsedStateSpecs.add(
            new StateSpec(stateName, Duration.ofMillis(optionalStateTtl.getAsLong())));
      } else {
        parsedStateSpecs.add(new StateSpec(stateName));
      }
    }

    return parsedStateSpecs;
  }

  private static FunctionType functionType(JsonNode functionNode) {
    String namespaceName = Selectors.textAt(functionNode, Pointers.Functions.META_TYPE);
    NamespaceNamePair nn = NamespaceNamePair.from(namespaceName);
    return new FunctionType(nn.namespace(), nn.name());
  }

  private static InetSocketAddress functionAddress(JsonNode functionNode) {
    String host = Selectors.textAt(functionNode, Pointers.Functions.FUNCTION_HOSTNAME);
    int port = Selectors.integerAt(functionNode, Pointers.Functions.FUNCTION_PORT);
    return new InetSocketAddress(host, port);
  }

  private static URI functionUri(JsonNode functionNode) {
    String uri = Selectors.textAt(functionNode, Pointers.Functions.FUNCTION_ENDPOINT);
    URI typedUri = URI.create(uri);
    @Nullable String scheme = typedUri.getScheme();
    if (scheme == null) {
      throw new IllegalArgumentException(
          "Missing scheme in function endpoint "
              + uri
              + "; an http or https scheme must be provided.");
    }
    if (scheme.equalsIgnoreCase("http")
        || scheme.equalsIgnoreCase("https")
        || scheme.equalsIgnoreCase("http+unix")
        || scheme.equalsIgnoreCase("https+unix")) {
      return typedUri;
    }
    throw new IllegalArgumentException(
        "Missing scheme in function endpoint "
            + uri
            + "; an http or https or http+unix or https+unix scheme must be provided.");
  }

  private static Collector<FunctionSpec, ?, Map<FunctionType, FunctionSpec>> groupByFunctionType() {
    return toMap(FunctionSpec::functionType, Function.identity());
  }
}
