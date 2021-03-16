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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.types.SimpleType;
import org.apache.flink.statefun.sdk.java.types.Type;

public class Messages {

  private static final ObjectMapper mapper = new ObjectMapper();

  public static final Type<RestockItem> RESTOCK_ITEM_TYPE =
      SimpleType.simpleImmutableTypeFrom(
          TypeName.typeNameFromString("com.example/RestockItem"),
          mapper::writeValueAsBytes,
          bytes -> mapper.readValue(bytes, RestockItem.class));

  public static final Type<RequestItem> REQUEST_ITEM_TYPE =
      SimpleType.simpleImmutableTypeFrom(
          TypeName.typeNameFromString("com.example/RequestItem"),
          mapper::writeValueAsBytes,
          bytes -> mapper.readValue(bytes, RequestItem.class));

  public static final Type<ItemAvailability> ITEM_AVAILABILITY_TYPE =
      SimpleType.simpleImmutableTypeFrom(
          TypeName.typeNameFromString("com.example/ItemAvailability"),
          mapper::writeValueAsBytes,
          bytes -> mapper.readValue(bytes, ItemAvailability.class));

  public static final Type<AddToCart> ADD_TO_CART =
      SimpleType.simpleImmutableTypeFrom(
          TypeName.typeNameFromString("com.example/AddToCart"),
          mapper::writeValueAsBytes,
          bytes -> mapper.readValue(bytes, AddToCart.class));

  public static final Type<ClearCart> CLEAR_CART_TYPE =
      SimpleType.simpleImmutableTypeFrom(
          TypeName.typeNameFromString("com.example/ClearCart"),
          mapper::writeValueAsBytes,
          bytes -> mapper.readValue(bytes, ClearCart.class));

  public static final Type<Checkout> CHECKOUT_TYPE =
      SimpleType.simpleImmutableTypeFrom(
          TypeName.typeNameFromString("com.example/Checkout"),
          mapper::writeValueAsBytes,
          bytes -> mapper.readValue(bytes, Checkout.class));

  public static final Type<Receipt> RECEIPT_TYPE =
      SimpleType.simpleImmutableTypeFrom(
          TypeName.typeNameFromString("com.example/Receipt"),
          mapper::writeValueAsBytes,
          bytes -> mapper.readValue(bytes, Receipt.class));

  public static class ClearCart {
    private final int userId;

    @JsonCreator
    public ClearCart(@JsonProperty("user_id") int userId) {
      this.userId = userId;
    }

    public int getUserId() {
      return userId;
    }

    @Override
    public String toString() {
      return "ClearCart{" + "userId=" + userId + '}';
    }
  }

  public static class Checkout {
    private final int userId;

    @JsonCreator
    public Checkout(@JsonProperty("user_id") int userId) {
      this.userId = userId;
    }

    public int getUserId() {
      return userId;
    }

    @Override
    public String toString() {
      return "Checkout{" + "userId=" + userId + '}';
    }
  }

  public static class Receipt {
    private final int userId;
    private final String details;

    public Receipt(@JsonProperty("user_id") int userId, String details) {
      this.userId = userId;
      this.details = details;
    }

    public int getUserId() {
      return userId;
    }

    public String getDetails() {
      return details;
    }

    @Override
    public String toString() {
      return "Receipt{" + "userId=" + userId + ", details='" + details + '\'' + '}';
    }
  }

  public static class RestockItem {
    private final String itemId;
    private final int quantity;

    @JsonCreator
    public RestockItem(@JsonProperty("item_id") String itemId, int quantity) {
      this.itemId = itemId;
      this.quantity = quantity;
    }

    public String getItemId() {
      return itemId;
    }

    public int getQuantity() {
      return quantity;
    }

    @Override
    public String toString() {
      return "RestockItem{" + "itemId='" + itemId + '\'' + ", quantity=" + quantity + '}';
    }
  }

  public static class AddToCart {
    private final int userId;
    private final int itemId;
    private final int quantity;

    @JsonCreator
    public AddToCart(
        @JsonProperty("user_id") int userId, @JsonProperty("item_id") int itemId, int quantity) {
      this.userId = userId;
      this.itemId = itemId;
      this.quantity = quantity;
    }

    public int getUserId() {
      return userId;
    }

    public int getItemId() {
      return itemId;
    }

    public int getQuantity() {
      return quantity;
    }

    @Override
    public String toString() {
      return "AddToCart{"
          + "userId="
          + userId
          + ", itemId="
          + itemId
          + ", quantity="
          + quantity
          + '}';
    }
  }

  // ---------------------------------------------------------------------
  // Internal messages
  // ---------------------------------------------------------------------

  public static class RequestItem {
    private final int quantity;

    @JsonCreator
    public RequestItem(int quantity) {
      this.quantity = quantity;
    }

    public int getQuantity() {
      return quantity;
    }

    @Override
    public String toString() {
      return "RequestItem{" + "quantity=" + quantity + '}';
    }
  }

  public static class ItemAvailability {

    public enum Status {
      INSTOCK,
      OUTOFSTOCK
    }

    private final Status status;
    private final int quantity;

    @JsonCreator
    public ItemAvailability(Status status, int quantity) {
      this.status = status;
      this.quantity = quantity;
    }

    public Status getStatus() {
      return status;
    }

    public int getQuantity() {
      return quantity;
    }

    @Override
    public String toString() {
      return "ItemAvailability{" + "status=" + status + ", quantity=" + quantity + '}';
    }
  }
}
