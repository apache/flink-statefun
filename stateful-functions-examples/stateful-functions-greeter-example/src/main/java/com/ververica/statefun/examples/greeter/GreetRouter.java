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

package com.ververica.statefun.examples.greeter;

import com.ververica.statefun.examples.greeter.generated.GreetRequest;
import com.ververica.statefun.sdk.io.Router;

/**
 * The greet router takes each message from an ingress and routes it to a greeter function based on
 * the users id.
 */
final class GreetRouter implements Router<GreetRequest> {

  @Override
  public void route(GreetRequest message, Downstream<GreetRequest> downstream) {
    downstream.forward(GreetStatefulFunction.TYPE, message.getWho(), message);
  }
}
