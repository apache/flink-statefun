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

package com.ververica.statefun.docs;

import com.ververica.statefun.sdk.spi.StatefulFunctionModule;
import java.util.Map;

public class BasicFunctionModule implements StatefulFunctionModule {

  public void configure(Map<String, String> globalConfiguration, Binder binder) {

    // Declare the user function and bind it to its type
    binder.bindFunctionProvider(FnWithDependency.TYPE, new CustomProvider());

    // Stateful functions that do not require any configuration
    // can declare their provider using java 8 lambda syntax
    binder.bindFunctionProvider(FnUser.TYPE, unused -> new FnUser());
  }
}
