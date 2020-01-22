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

import java.util.Map;
import org.apache.flink.statefun.examples.shoppingcart.generated.ProtobufMessages;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.PersistedValue;

final class UserShoppingCart implements StatefulFunction {

  @Persisted
  private final PersistedValue<ProtobufMessages.Basket> userBasket =
      PersistedValue.of("basket", ProtobufMessages.Basket.class);

  @Override
  public void invoke(Context context, Object input) {
    if (input instanceof ProtobufMessages.AddToCart) {
      ProtobufMessages.AddToCart addToCart = (ProtobufMessages.AddToCart) input;
      ProtobufMessages.RequestItem request =
          ProtobufMessages.RequestItem.newBuilder().setQuantity(addToCart.getQuantity()).build();
      Address address = new Address(Identifiers.INVENTORY, addToCart.getItemId());
      context.send(address, request);
    }

    if (input instanceof ProtobufMessages.ItemAvailability) {
      ProtobufMessages.ItemAvailability availability = (ProtobufMessages.ItemAvailability) input;

      if (availability.getStatus() == ProtobufMessages.ItemAvailability.Status.INSTOCK) {
        ProtobufMessages.Basket basket =
            userBasket.getOrDefault(() -> ProtobufMessages.Basket.newBuilder().build());
        basket.getItemsMap().put(context.caller().id(), availability.getQuantity());
      }
    }

    if (input instanceof ProtobufMessages.ClearCart) {
      ProtobufMessages.Basket basket = userBasket.get();
      if (basket == null) {
        return;
      }

      for (Map.Entry<String, Integer> entry : basket.getItemsMap().entrySet()) {
        ProtobufMessages.RestockItem item =
            ProtobufMessages.RestockItem.newBuilder()
                .setItemId(entry.getKey())
                .setQuantity(entry.getValue())
                .build();

        Address address = new Address(Identifiers.INVENTORY, entry.getKey());
        context.send(address, item);
      }

      userBasket.clear();
    }

    if (input instanceof ProtobufMessages.Checkout) {
      ProtobufMessages.Basket basket = userBasket.get();
      if (basket == null) {
        return;
      }

      ProtobufMessages.Receipt receipt =
          ProtobufMessages.Receipt.newBuilder()
              .setUserId(context.self().id())
              .setDetails("You bought " + basket.getItemsCount() + " items")
              .build();

      context.send(Identifiers.RECEIPT, receipt);
      userBasket.clear();
    }
  }
}
