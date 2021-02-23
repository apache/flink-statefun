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

package org.apache.flink.statefun.flink.core.protorouter;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.apache.flink.statefun.flink.io.generated.AutoRoutable;
import org.apache.flink.statefun.flink.io.generated.RoutingConfig;
import org.apache.flink.statefun.flink.io.generated.TargetFunctionType;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.Router;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

/**
 * A {@link Router} that recognizes messages of type {@link AutoRoutable}.
 *
 * <p>For each incoming {@code AutoRoutable}, this router forwards the wrapped payload to the
 * configured target addresses as a {@link TypedValue} message.
 */
public final class AutoRoutableProtobufRouter implements Router<Message> {

  /**
   * Note: while the input and type of this method is both {@link Message}, we actually do a
   * conversion here. The input {@link Message} is an {@link AutoRoutable}, which gets converted to
   * a {@link TypedValue} as the output after slicing the target address and actual payload.
   */
  @Override
  public void route(Message message, Downstream<Message> downstream) {
    final AutoRoutable routable = asAutoRoutable(message);
    final RoutingConfig config = routable.getConfig();
    for (TargetFunctionType targetFunction : config.getTargetFunctionTypesList()) {
      downstream.forward(
          sdkFunctionType(targetFunction),
          routable.getId(),
          typedValuePayload(config.getTypeUrl(), routable.getPayloadBytes()));
    }
  }

  private static AutoRoutable asAutoRoutable(Message message) {
    try {
      return (AutoRoutable) message;
    } catch (ClassCastException e) {
      throw new RuntimeException(
          "This router only expects messages of type " + AutoRoutable.class.getName(), e);
    }
  }

  private FunctionType sdkFunctionType(TargetFunctionType targetFunctionType) {
    return new FunctionType(targetFunctionType.getNamespace(), targetFunctionType.getType());
  }

  private static TypedValue typedValuePayload(String typeUrl, ByteString payloadBytes) {
    return TypedValue.newBuilder()
        .setTypename(typeUrl)
        .setHasValue(true)
        .setValue(payloadBytes)
        .build();
  }
}
