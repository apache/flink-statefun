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

package com.ververica.statefun.docs.io.egress;

import com.ververica.statefun.docs.io.MissingImplementationException;
import com.ververica.statefun.docs.models.User;
import com.ververica.statefun.sdk.io.EgressIdentifier;
import com.ververica.statefun.sdk.io.EgressSpec;
import com.ververica.statefun.sdk.spi.StatefulFunctionModule;
import java.util.Map;

public class ModuleWithEgress implements StatefulFunctionModule {

  @Override
  public void configure(Map<String, String> globalConfiguration, Binder binder) {
    EgressSpec<User> spec = createEgress(Identifiers.EGRESS);
    binder.bindEgress(spec);
  }

  public EgressSpec<User> createEgress(EgressIdentifier<User> identifier) {
    throw new MissingImplementationException("Replace with your specific egress");
  }
}
