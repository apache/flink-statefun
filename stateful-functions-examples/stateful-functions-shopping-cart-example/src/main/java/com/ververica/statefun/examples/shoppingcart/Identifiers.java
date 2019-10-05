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

import com.ververica.statefun.examples.shoppingcart.generated.ProtobufMessages;
import com.ververica.statefun.sdk.FunctionType;
import com.ververica.statefun.sdk.io.EgressIdentifier;
import com.ververica.statefun.sdk.io.IngressIdentifier;

final class Identifiers {

  private Identifiers() {}

  static final FunctionType USER = new FunctionType("shopping-cart", "user");

  static final FunctionType INVENTORY = new FunctionType("shopping-cart", "inventory");

  static final IngressIdentifier<ProtobufMessages.RestockItem> RESTOCK =
      new IngressIdentifier<>(ProtobufMessages.RestockItem.class, "shopping-cart", "restock-item");

  static final EgressIdentifier<ProtobufMessages.Receipt> RECEIPT =
      new EgressIdentifier<>("shopping-cart", "receipt", ProtobufMessages.Receipt.class);
}
