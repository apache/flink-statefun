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

import com.ververica.statefun.examples.shoppingcart.generated.ProtobufMessages.Receipt;
import com.ververica.statefun.examples.shoppingcart.generated.ProtobufMessages.RestockItem;
import com.ververica.statefun.sdk.io.EgressSpec;
import com.ververica.statefun.sdk.io.IngressSpec;
import com.ververica.statefun.sdk.kafka.KafkaEgressBuilder;
import com.ververica.statefun.sdk.kafka.KafkaIngressBuilder;
import com.ververica.statefun.sdk.spi.StatefulFunctionModule;
import java.util.Map;

public class ShoppingCartModule implements StatefulFunctionModule {
  @Override
  public void configure(Map<String, String> globalConfiguration, Binder binder) {
    // bind functions
    binder.bindFunctionProvider(Identifiers.USER, unused -> new UserShoppingCart());
    binder.bindFunctionProvider(Identifiers.INVENTORY, unused -> new Inventory());

    // For ingress and egress pretend I filled in the details :)

    IngressSpec<RestockItem> restockSpec =
        KafkaIngressBuilder.forIdentifier(Identifiers.RESTOCK).build();

    binder.bindIngress(restockSpec);

    binder.bindIngressRouter(Identifiers.RESTOCK, new RestockRouter());

    EgressSpec<Receipt> receiptSpec = KafkaEgressBuilder.forIdentifier(Identifiers.RECEIPT).build();

    binder.bindEgress(receiptSpec);
  }
}
