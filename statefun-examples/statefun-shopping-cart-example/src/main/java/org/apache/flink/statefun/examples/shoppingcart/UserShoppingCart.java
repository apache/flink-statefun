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
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.statefun.examples.shoppingcart.generated.ProtobufMessages;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.PersistedTable;

final class UserShoppingCart implements StatefulFunction {

  @Persisted
  private final PersistedTable<String, Integer> userBasket =
      PersistedTable.of("basket", String.class, Integer.class);

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
        userBasket.set(context.caller().id(), availability.getQuantity());
      }
    }

    if (input instanceof ProtobufMessages.ClearCart) {
      for (Map.Entry<String, Integer> entry : userBasket.entries()) {
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

      String items =
          StreamSupport.stream(userBasket.entries().spliterator(), false)
              .map(entry -> entry.getKey() + ": " + entry.getValue())
              .collect(Collectors.joining("\n"));

      ProtobufMessages.Receipt receipt =
          ProtobufMessages.Receipt.newBuilder()
              .setUserId(context.self().id())
              .setDetails(items)
              .build();

      context.send(Identifiers.RECEIPT, receipt);
      userBasket.clear();
    }
  }
}
