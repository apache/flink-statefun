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
package com.ververica.statefun.flink.core.protorouter;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.ververica.statefun.flink.common.protopath.ProtobufPath;
import com.ververica.statefun.sdk.Address;
import com.ververica.statefun.sdk.FunctionType;
import java.util.List;
import java.util.Objects;

final class AddressResolver {

  /**
   * Creates an address resolver that is able to produce {@link Address} from an address template
   * and a Protocol Buffers message.
   *
   * <p>An address template is an address of the form function-namespace/function-type/function-id.
   * where each component can contain multiple {@link ProtobufPath} expressions.
   *
   * @param messageDescriptor the protocol buffers message descriptor that would be used to extract
   *     the target address from.
   * @param addressTemplate the template that would be used to extract the target address by.
   * @return an instance of an address evaluator, that is able to produce an {@link Address} given a
   *     Protocol Buffers message.
   */
  static AddressResolver fromAddressTemplate(
      Descriptors.Descriptor messageDescriptor, String addressTemplate) {
    Objects.requireNonNull(messageDescriptor);
    Objects.requireNonNull(addressTemplate);

    int lastSlash = addressTemplate.lastIndexOf("/");
    if (lastSlash <= 0) {
      throw new IllegalArgumentException(
          "The address template is not of the form <function type>/<id>");
    }
    String functionTypeTemplate = addressTemplate.substring(0, lastSlash);
    String idTemplate = addressTemplate.substring(lastSlash + 1);
    if (idTemplate.isEmpty()) {
      throw new IllegalArgumentException(
          "The address template is not of the form <function type>/<id>");
    }
    lastSlash = functionTypeTemplate.lastIndexOf("/");
    if (lastSlash <= 0) {
      throw new IllegalArgumentException(
          "The function type template is not of the form <function namespace>/<function name>");
    }
    String functionNamespaceTemplate = functionTypeTemplate.substring(0, lastSlash);
    String functionNameIdTemplate = functionTypeTemplate.substring(lastSlash + 1);
    if (functionNameIdTemplate.isEmpty()) {
      throw new IllegalArgumentException(
          "The address template is not of the form <function type>/<id>");
    }
    return new AddressResolver(
        evaluator(messageDescriptor, functionNamespaceTemplate),
        evaluator(messageDescriptor, functionNameIdTemplate),
        evaluator(messageDescriptor, idTemplate));
  }

  static TemplateEvaluator evaluator(Descriptors.Descriptor descriptor, String template) {
    List<TemplateParser.TextFragment> fragments = TemplateParser.parseTemplateString(template);
    return new TemplateEvaluator(descriptor, fragments);
  }

  private final TemplateEvaluator functionNamespace;
  private final TemplateEvaluator functionName;
  private final TemplateEvaluator functionId;

  private AddressResolver(
      TemplateEvaluator functionNamespace,
      TemplateEvaluator functionName,
      TemplateEvaluator functionId) {
    this.functionNamespace = Objects.requireNonNull(functionNamespace);
    this.functionName = Objects.requireNonNull(functionName);
    this.functionId = Objects.requireNonNull(functionId);
  }

  Address evaluate(Message message) {
    FunctionType functionType =
        new FunctionType(functionNamespace.evaluate(message), functionName.evaluate(message));

    return new Address(functionType, functionId.evaluate(message));
  }
}
