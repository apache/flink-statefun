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

import static org.apache.flink.statefun.examples.shoppingcart.Identifiers.RECEIPT_EGRESS;
import static org.apache.flink.statefun.examples.shoppingcart.Messages.*;
import static org.apache.flink.statefun.examples.shoppingcart.Messages.ItemAvailability.*;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.flink.statefun.sdk.java.AddressScopedStorage;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.ValueSpec;
import org.apache.flink.statefun.sdk.java.io.KafkaEgressMessage;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import org.apache.flink.statefun.sdk.java.types.SimpleType;
import org.apache.flink.statefun.sdk.java.types.Type;

final class UserShoppingCartFn implements StatefulFunction {

  static final TypeName TYPE = TypeName.typeNameFromString("com.example.fns/user-shopping-cart");

  static final ValueSpec<Basket> BASKET = ValueSpec.named("basket").withCustomType(Basket.TYPE);

  @Override
  public CompletableFuture<Void> apply(Context context, Message message) {
    if (message.is(ADD_TO_CART)) {
      AddToCart addToCart = message.as(ADD_TO_CART);

      RequestItem requestItem = new RequestItem(addToCart.getQuantity());
      Message request =
          MessageBuilder.forAddress(InventoryFn.TYPE, Integer.toString(addToCart.getItemId()))
              .withCustomType(REQUEST_ITEM_TYPE, requestItem)
              .build();
      context.send(request);
    }

    if (message.is(ITEM_AVAILABILITY_TYPE)) {
      ItemAvailability availability = message.as(ITEM_AVAILABILITY_TYPE);
      if (Status.INSTOCK.equals(availability.getStatus())) {
        AddressScopedStorage storage = context.storage();
        Basket basket = storage.get(BASKET).orElse(Basket.initEmpty());
        // ItemAvailability event comes from the Inventory function and contains the itemId as the
        // caller id
        context.caller().ifPresent(caller -> basket.add(caller.id(), availability.getQuantity()));
      }
    }

    if (message.is(CLEAR_CART_TYPE)) {
      AddressScopedStorage storage = context.storage();
      storage
          .get(BASKET)
          .ifPresent(
              basket -> {
                for (Map.Entry<String, Integer> entry : basket.getEntries()) {
                  RestockItem restockItem = new RestockItem(entry.getKey(), entry.getValue());
                  Message restockCommand =
                      MessageBuilder.forAddress(InventoryFn.TYPE, entry.getKey())
                          .withCustomType(RESTOCK_ITEM_TYPE, restockItem)
                          .build();

                  context.send(restockCommand);
                }
                basket.clear();
              });
    }

    if (message.is(CHECKOUT_TYPE)) {
      AddressScopedStorage storage = context.storage();
      Optional<String> itemsOption =
          storage
              .get(BASKET)
              .map(
                  basket ->
                      basket.getEntries().stream()
                          .map(entry -> entry.getKey() + ": " + entry.getValue())
                          .collect(Collectors.joining("\n")));

      itemsOption.ifPresent(
          items -> {
            Receipt receipt = new Receipt(Integer.parseInt(context.self().id()), items);
            KafkaEgressMessage.forEgress(RECEIPT_EGRESS)
                .withTopic("receipts")
                .withUtf8Key(context.self().id())
                .withValue(RECEIPT_TYPE, receipt)
                .build();
          });
    }
    return context.done();
  }

  private static class Basket {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static final Type<Basket> TYPE =
        SimpleType.simpleImmutableTypeFrom(
            TypeName.typeNameFromString("com.example/Basket"),
            mapper::writeValueAsBytes,
            bytes -> mapper.readValue(bytes, Basket.class));

    private final Map<String, Integer> basket;

    public static Basket initEmpty() {
      return new Basket(new HashMap<>());
    }

    @JsonCreator
    public Basket(Map<String, Integer> basket) {
      this.basket = basket;
    }

    public void add(String itemId, int quantity) {
      basket.put(itemId, basket.getOrDefault(itemId, 0) + quantity);
    }

    public void remove(String itemId, int quantity) {
      int remainder = basket.getOrDefault(itemId, 0) - quantity;
      if (remainder > 0) {
        basket.put(itemId, remainder);
      } else {
        basket.remove(itemId);
      }
    }

    public Set<Map.Entry<String, Integer>> getEntries() {
      return basket.entrySet();
    }

    public void clear() {
      basket.clear();
    }

    @Override
    public String toString() {
      return "Basket{" + "basket=" + basket + '}';
    }
  }
}
