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

package com.ververica.statefun.examples.shoppingcart;

import com.ververica.statefun.examples.shoppingcart.generated.ProtobufMessages.AddToCart;
import com.ververica.statefun.examples.shoppingcart.generated.ProtobufMessages.Basket;
import com.ververica.statefun.examples.shoppingcart.generated.ProtobufMessages.Checkout;
import com.ververica.statefun.examples.shoppingcart.generated.ProtobufMessages.ClearCart;
import com.ververica.statefun.examples.shoppingcart.generated.ProtobufMessages.ItemAvailability;
import com.ververica.statefun.examples.shoppingcart.generated.ProtobufMessages.Receipt;
import com.ververica.statefun.examples.shoppingcart.generated.ProtobufMessages.RequestItem;
import com.ververica.statefun.examples.shoppingcart.generated.ProtobufMessages.RestockItem;
import com.ververica.statefun.sdk.Address;
import com.ververica.statefun.sdk.Context;
import com.ververica.statefun.sdk.StatefulFunction;
import com.ververica.statefun.sdk.annotations.Persisted;
import com.ververica.statefun.sdk.state.PersistedValue;
import java.util.Map;

final class UserShoppingCart implements StatefulFunction {

  @Persisted
  private final PersistedValue<Basket> userBasket = PersistedValue.of("basket", Basket.class);

  @Override
  public void invoke(Context context, Object input) {
    if (input instanceof AddToCart) {
      AddToCart addToCart = (AddToCart) input;
      RequestItem request = RequestItem.newBuilder().setQuantity(addToCart.getQuantity()).build();
      Address address = new Address(Identifiers.INVENTORY, addToCart.getItemId());
      context.send(address, request);
    }

    if (input instanceof ItemAvailability) {
      ItemAvailability availability = (ItemAvailability) input;

      if (availability.getStatus() == ItemAvailability.Status.INSTOCK) {
        Basket basket = userBasket.getOrDefault(() -> Basket.newBuilder().build());
        basket.getItemsMap().put(context.caller().id(), availability.getQuantity());
      }
    }

    if (input instanceof ClearCart) {
      Basket basket = userBasket.get();
      if (basket == null) {
        return;
      }

      for (Map.Entry<String, Integer> entry : basket.getItemsMap().entrySet()) {
        RestockItem item =
            RestockItem.newBuilder()
                .setItemId(entry.getKey())
                .setQuantity(entry.getValue())
                .build();

        Address address = new Address(Identifiers.INVENTORY, entry.getKey());
        context.send(address, item);
      }

      userBasket.clear();
    }

    if (input instanceof Checkout) {
      Basket basket = userBasket.get();
      if (basket == null) {
        return;
      }

      Receipt receipt =
          Receipt.newBuilder()
              .setUserId(context.self().id())
              .setDetails("You bought " + basket.getItemsCount() + " items")
              .build();

      context.send(Identifiers.RECEIPT, receipt);
      userBasket.clear();
    }
  }
}
