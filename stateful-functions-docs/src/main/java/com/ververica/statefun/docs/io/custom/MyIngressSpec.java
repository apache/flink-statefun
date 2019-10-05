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

package com.ververica.statefun.docs.io.custom;

import com.ververica.statefun.sdk.IngressType;
import com.ververica.statefun.sdk.io.IngressIdentifier;
import com.ververica.statefun.sdk.io.IngressSpec;

public class MyIngressSpec<T> implements IngressSpec<T> {

  public static final IngressType TYPE = new IngressType("ververica", "my-ingress");

  private final IngressIdentifier<T> identifier;

  public MyIngressSpec(IngressIdentifier<T> identifier) {
    this.identifier = identifier;
  }

  @Override
  public IngressType type() {
    return TYPE;
  }

  @Override
  public IngressIdentifier<T> id() {
    return identifier;
  }
}
