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

package com.ververica.statefun.docs.match;

import com.ververica.statefun.sdk.Context;
import com.ververica.statefun.sdk.match.MatchBinder;
import com.ververica.statefun.sdk.match.StatefulMatchFunction;

public class FnMatchGreeterWithCatchAll extends StatefulMatchFunction {

  @Override
  public void configure(MatchBinder binder) {
    binder
        .predicate(Customer.class, this::greetCustomer)
        .predicate(Employee.class, Employee::isManager, this::greetManager)
        .predicate(Employee.class, this::greetEmployee)
        .otherwise(this::catchAll);
  }

  private void catchAll(Context context, Object message) {
    System.out.println("Hello unexpected message");
  }

  private void greetManager(Context context, Employee message) {
    System.out.println("Hello manager");
  }

  private void greetEmployee(Context context, Employee message) {
    System.out.println("Hello employee");
  }

  private void greetCustomer(Context context, Customer message) {
    System.out.println("Hello customer");
  }
}
