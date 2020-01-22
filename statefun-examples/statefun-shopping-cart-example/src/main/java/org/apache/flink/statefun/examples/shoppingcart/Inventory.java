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
package org.apache.flink.statefun.examples.shoppingcart;

import org.apache.flink.statefun.examples.shoppingcart.generated.ProtobufMessages;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.PersistedValue;

final class Inventory implements StatefulFunction {

  @Persisted
  private final PersistedValue<Integer> inventory = PersistedValue.of("inventory", Integer.class);

  @Override
  public void invoke(Context context, Object message) {
    if (message instanceof ProtobufMessages.RestockItem) {
      int quantity =
          inventory.getOrDefault(0) + ((ProtobufMessages.RestockItem) message).getQuantity();
      inventory.set(quantity);
    } else if (message instanceof ProtobufMessages.RequestItem) {
      int quantity = inventory.getOrDefault(0);
      int requestedAmount = ((ProtobufMessages.RequestItem) message).getQuantity();

      ProtobufMessages.ItemAvailability.Builder availability =
          ProtobufMessages.ItemAvailability.newBuilder().setQuantity(requestedAmount);

      if (quantity >= requestedAmount) {
        inventory.set(quantity - requestedAmount);
        availability.setStatus(ProtobufMessages.ItemAvailability.Status.INSTOCK);
      } else {
        availability.setStatus(ProtobufMessages.ItemAvailability.Status.OUTOFSTOCK);
      }

      context.send(context.caller(), availability.build());
    }
  }
}
