package org.apache.flink.statefun.flink.core.jsonmodule;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;

import com.google.protobuf.Any;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.statefun.flink.common.ResourceLocator;
import org.apache.flink.statefun.flink.common.json.NamespaceNamePair;
import org.apache.flink.statefun.flink.common.json.Selectors;
import org.apache.flink.statefun.flink.common.protobuf.ProtobufDescriptorMap;
import org.apache.flink.statefun.flink.core.grpcfn.GrpcFunctionSpec;
import org.apache.flink.statefun.flink.core.httpfn.HttpFunctionSpec;
import org.apache.flink.statefun.flink.core.protorouter.ProtobufRouter;
import org.apache.flink.statefun.flink.io.spi.JsonEgressSpec;
import org.apache.flink.statefun.flink.io.spi.JsonIngressSpec;
import org.apache.flink.statefun.sdk.EgressType;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.IngressType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.Router;
import org.apache.flink.util.TimeUtils;

/** {@link JsonModuleSpecParser} implementation for format version {@link FormatVersion#v1_0}. */
class JsonModuleSpecParserV1 extends JsonModuleSpecParser {

  JsonModuleSpecParserV1(JsonNode spec) {
    super(spec);
  }

  @Override
  protected Map<FunctionSpec.Kind, Map<FunctionType, FunctionSpec>> parseFunctions() {
    final Iterable<? extends JsonNode> functions =
        Selectors.listAt(spec, Pointers.FUNCTIONS_POINTER);

    return StreamSupport.stream(functions.spliterator(), false)
        .map(JsonModuleSpecParserV1::parseFunctionSpec)
        .collect(groupingBy(FunctionSpec::kind, groupByFunctionType()));
  }

  @Override
  protected Iterable<Tuple2<IngressIdentifier<Message>, Router<Message>>> parseRouters() {
    final Iterable<? extends JsonNode> routerNodes =
        Selectors.listAt(spec, Pointers.ROUTERS_POINTER);

    final List<Tuple2<IngressIdentifier<Message>, Router<Message>>> routers = new ArrayList<>();
    routerNodes.forEach(
        routerNode -> {
          // currently the only type of router supported in a module.yaml, is a protobuf
          // dynamicMessage
          // router once we will introduce further router types we should refactor this to be more
          // dynamic.
          requireProtobufRouterType(routerNode);

          routers.add(Tuple2.of(targetRouterIngress(routerNode), dynamicRouter(routerNode)));
        });

    return routers;
  }

  @Override
  protected Iterable<JsonIngressSpec<Message>> parseIngressSpecs() {
    final Iterable<? extends JsonNode> ingressNodes =
        Selectors.listAt(spec, Pointers.INGRESSES_POINTER);

    final List<JsonIngressSpec<Message>> ingressSpecs = new ArrayList<>();
    ingressNodes.forEach(
        ingressNode -> {
          ingressSpecs.add(
              new JsonIngressSpec<>(ingressType(ingressNode), ingressId(ingressNode), ingressNode));
        });
    return ingressSpecs;
  }

  @Override
  protected Iterable<JsonEgressSpec<Any>> parseEgressSpecs() {
    final Iterable<? extends JsonNode> egressNodes =
        Selectors.listAt(spec, Pointers.EGRESSES_POINTER);

    final List<JsonEgressSpec<Any>> egressSpecs = new ArrayList<>();
    egressNodes.forEach(
        egressNode -> {
          egressSpecs.add(
              new JsonEgressSpec<>(egressType(egressNode), egressId(egressNode), egressNode));
        });
    return egressSpecs;
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

        for (String state : functionStates(functionNode)) {
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

  private static List<String> functionStates(JsonNode functionNode) {
    return Selectors.textListAt(functionNode, Pointers.Functions.FUNCTION_STATES);
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

  // ----------------------------------------------------------------------------------------------------------
  // Ingresses
  // ----------------------------------------------------------------------------------------------------------

  private static IngressType ingressType(JsonNode spec) {
    String typeString = Selectors.textAt(spec, Pointers.Ingress.META_TYPE);
    NamespaceNamePair nn = NamespaceNamePair.from(typeString);
    return new IngressType(nn.namespace(), nn.name());
  }

  private static IngressIdentifier<Message> ingressId(JsonNode ingress) {
    String ingressId = Selectors.textAt(ingress, Pointers.Ingress.META_ID);
    NamespaceNamePair nn = NamespaceNamePair.from(ingressId);
    return new IngressIdentifier<>(Message.class, nn.namespace(), nn.name());
  }

  // ----------------------------------------------------------------------------------------------------------
  // Routers
  // ----------------------------------------------------------------------------------------------------------

  private static Router<Message> dynamicRouter(JsonNode router) {
    String addressTemplate = Selectors.textAt(router, Pointers.Routers.SPEC_TARGET);
    String descriptorSetPath = Selectors.textAt(router, Pointers.Routers.SPEC_DESCRIPTOR);
    String messageType = Selectors.textAt(router, Pointers.Routers.SPEC_MESSAGE_TYPE);

    ProtobufDescriptorMap descriptorPath = protobufDescriptorMap(descriptorSetPath);
    Optional<Descriptors.GenericDescriptor> maybeDescriptor =
        descriptorPath.getDescriptorByName(messageType);
    if (!maybeDescriptor.isPresent()) {
      throw new IllegalStateException(
          "Error while processing a router definition. Unable to locate a message "
              + messageType
              + " in a descriptor set "
              + descriptorSetPath);
    }
    return ProtobufRouter.forAddressTemplate(
        (Descriptors.Descriptor) maybeDescriptor.get(), addressTemplate);
  }

  private static ProtobufDescriptorMap protobufDescriptorMap(String descriptorSetPath) {
    try {
      URL url = ResourceLocator.findNamedResource(descriptorSetPath);
      if (url == null) {
        throw new IllegalArgumentException(
            "Unable to locate a Protobuf descriptor set at " + descriptorSetPath);
      }
      return ProtobufDescriptorMap.from(url);
    } catch (IOException e) {
      throw new IllegalStateException(
          "Error while processing a router definition. Unable to read the descriptor set at  "
              + descriptorSetPath,
          e);
    }
  }

  private static IngressIdentifier<Message> targetRouterIngress(JsonNode routerNode) {
    String targetIngress = Selectors.textAt(routerNode, Pointers.Routers.SPEC_INGRESS);
    NamespaceNamePair nn = NamespaceNamePair.from(targetIngress);
    return new IngressIdentifier<>(Message.class, nn.namespace(), nn.name());
  }

  private static void requireProtobufRouterType(JsonNode routerNode) {
    String routerType = Selectors.textAt(routerNode, Pointers.Routers.META_TYPE);
    if (!routerType.equalsIgnoreCase("org.apache.flink.statefun.sdk/protobuf-router")) {
      throw new IllegalStateException("Invalid router type " + routerType);
    }
  }

  // ----------------------------------------------------------------------------------------------------------
  // Egresses
  // ----------------------------------------------------------------------------------------------------------

  private static EgressType egressType(JsonNode spec) {
    String typeString = Selectors.textAt(spec, Pointers.Egress.META_TYPE);
    NamespaceNamePair nn = NamespaceNamePair.from(typeString);
    return new EgressType(nn.namespace(), nn.name());
  }

  private static EgressIdentifier<Any> egressId(JsonNode spec) {
    String egressId = Selectors.textAt(spec, Pointers.Egress.META_ID);
    NamespaceNamePair nn = NamespaceNamePair.from(egressId);
    return new EgressIdentifier<>(nn.namespace(), nn.name(), Any.class);
  }
}
