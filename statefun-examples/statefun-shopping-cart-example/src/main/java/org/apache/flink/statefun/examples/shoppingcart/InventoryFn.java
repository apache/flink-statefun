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

import static org.apache.flink.statefun.examples.shoppingcart.Messages.*;
import static org.apache.flink.statefun.examples.shoppingcart.Messages.ItemAvailability.*;

import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.java.AddressScopedStorage;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.ValueSpec;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;

final class InventoryFn implements StatefulFunction {

  static final TypeName TYPE = TypeName.typeNameFromString("com.example.fns/inventory");

  static final ValueSpec<Integer> INVENTORY = ValueSpec.named("inventory").withIntType();

  @Override
  public CompletableFuture<Void> apply(Context context, Message message) {
    AddressScopedStorage storage = context.storage();
    int quantity = storage.get(INVENTORY).orElse(0);
    if (message.is(RESTOCK_ITEM_TYPE)) {
      RestockItem restock = message.as(RESTOCK_ITEM_TYPE);
      int newQuantity = quantity + restock.getQuantity();
      storage.set(INVENTORY, newQuantity);
    } else if (message.is(REQUEST_ITEM_TYPE)) {
      RequestItem request = message.as(REQUEST_ITEM_TYPE);
      int requestQuantity = request.getQuantity();

      ItemAvailability itemAvailability;

      if (quantity >= requestQuantity) {
        storage.set(INVENTORY, quantity - requestQuantity);
        itemAvailability = new ItemAvailability(Status.INSTOCK, requestQuantity);
      } else {
        itemAvailability = new ItemAvailability(Status.OUTOFSTOCK, requestQuantity);
      }

      context
          .caller()
          .ifPresent(
              caller ->
                  context.send(
                      MessageBuilder.forAddress(caller)
                          .withCustomType(ITEM_AVAILABILITY_TYPE, itemAvailability)
                          .build()));
    }
    return context.done();
  }
}
