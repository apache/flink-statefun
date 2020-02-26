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

package org.apache.flink.statefun.e2e.routablekafka;

import com.google.protobuf.Any;
import org.apache.flink.statefun.e2e.routablekafka.generated.RoutableKafkaVerification.FnAddress;
import org.apache.flink.statefun.e2e.routablekafka.generated.RoutableKafkaVerification.MessageWithAddress;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;

public final class FnSelfAddressTagger implements StatefulFunction {

  @Override
  public void invoke(Context context, Object input) {
    MessageWithAddress message = cast(input);
    context.send(Constants.EGRESS_ID, tagWithSelfAddress(message, context));
  }

  private static MessageWithAddress cast(Object input) {
    Any any = (Any) input;
    try {
      return any.unpack(MessageWithAddress.class);
    } catch (Exception e) {
      throw new RuntimeException("Unable to unpack input as MessageWithAddress.", e);
    }
  }

  private MessageWithAddress tagWithSelfAddress(MessageWithAddress original, Context context) {
    return original.toBuilder().setFrom(fnAddress(context.self())).build();
  }

  private FnAddress fnAddress(Address sdkAddress) {
    return FnAddress.newBuilder()
        .setNamespace(sdkAddress.type().namespace())
        .setType(sdkAddress.type().name())
        .setId(sdkAddress.id())
        .build();
  }
}
