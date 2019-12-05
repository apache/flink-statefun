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
import com.google.protobuf.DynamicMessage;
import com.ververica.statefun.flink.core.types.protobuf.protopath.ProtobufPath;
import com.ververica.statefun.sdk.Address;
import com.ververica.statefun.sdk.io.Router;
import java.util.Objects;

/**
 * Routes {@code Protocol Buffers} {@link DynamicMessage}s based on an address template string.
 *
 * <p>Route messages downstream to an address determined by an address template of the form
 * target-function-namespace/target-function-type/target-function-id. Each address template
 * component can reference a section of the input {@link DynamicMessage} by using {@link
 * ProtobufPath} expressions.
 *
 * <p>For example consider the following {@code Protocol Buffers} message type
 *
 * <pre>{@code
 * message MyInput {
 *     string name = 1;
 * }
 * }</pre>
 *
 * And an instance of the message {@code { "name" : "bob" }}, and the following template string:
 * "com.ververica/python-function/{{$.name}}".
 *
 * <p>This message would be routed to the address: {@code Address(FunctionType(com.ververica,
 * python-function), bob)}.
 */
public final class ProtobufRouter implements Router<DynamicMessage> {

  public static ProtobufRouter forAddressTemplate(
      Descriptors.Descriptor descriptor, String addressTemplate) {
    AddressResolver evaluator = AddressResolver.fromAddressTemplate(descriptor, addressTemplate);
    return new ProtobufRouter(evaluator);
  }

  private final AddressResolver addressResolver;

  private ProtobufRouter(AddressResolver addressResolver) {
    this.addressResolver = Objects.requireNonNull(addressResolver);
  }

  @Override
  public void route(DynamicMessage message, Downstream<DynamicMessage> downstream) {
    Address targetAddress = addressResolver.evaluate(message);
    downstream.forward(targetAddress, message);
  }
}
