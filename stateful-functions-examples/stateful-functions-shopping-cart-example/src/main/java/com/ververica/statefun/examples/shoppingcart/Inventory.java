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

import com.ververica.statefun.examples.shoppingcart.generated.ProtobufMessages.ItemAvailability;
import com.ververica.statefun.examples.shoppingcart.generated.ProtobufMessages.RequestItem;
import com.ververica.statefun.examples.shoppingcart.generated.ProtobufMessages.RestockItem;
import com.ververica.statefun.sdk.Context;
import com.ververica.statefun.sdk.StatefulFunction;
import com.ververica.statefun.sdk.annotations.Persisted;
import com.ververica.statefun.sdk.state.PersistedValue;

final class Inventory implements StatefulFunction {

  @Persisted
  private final PersistedValue<Integer> inventory = PersistedValue.of("inventory", Integer.class);

  @Override
  public void invoke(Context context, Object message) {
    if (message instanceof RestockItem) {
      int quantity = inventory.getOrDefault(0) + ((RestockItem) message).getQuantity();
      inventory.set(quantity);
    } else if (message instanceof RequestItem) {
      int quantity = inventory.getOrDefault(0);
      int requestedAmount = ((RequestItem) message).getQuantity();

      ItemAvailability.Builder availability =
          ItemAvailability.newBuilder().setQuantity(requestedAmount);

      if (quantity >= requestedAmount) {
        inventory.set(quantity - requestedAmount);
        availability.setStatus(ItemAvailability.Status.INSTOCK);
      } else {
        availability.setStatus(ItemAvailability.Status.OUTOFSTOCK);
      }

      context.send(context.caller(), availability.build());
    }
  }
}
