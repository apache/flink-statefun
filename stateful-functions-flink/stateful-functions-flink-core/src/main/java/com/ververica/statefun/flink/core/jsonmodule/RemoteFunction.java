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
package com.ververica.statefun.flink.core.jsonmodule;

import com.ververica.statefun.sdk.Context;
import com.ververica.statefun.sdk.StatefulFunction;
import java.util.Objects;

public class RemoteFunction implements StatefulFunction {
  private final RemoteFunctionSpec functionSpec;

  public RemoteFunction(RemoteFunctionSpec functionSpec) {
    this.functionSpec = Objects.requireNonNull(functionSpec);
  }

  @Override
  public void invoke(Context context, Object input) {
    throw new UnsupportedOperationException(functionSpec.functionType() + " is not yet supported.");
  }
}
