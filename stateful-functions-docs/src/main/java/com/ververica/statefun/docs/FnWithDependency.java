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

import com.ververica.statefun.docs.dependency.RuntimeDependency;
import com.ververica.statefun.sdk.Context;
import com.ververica.statefun.sdk.FunctionType;
import com.ververica.statefun.sdk.StatefulFunction;

public class FnWithDependency implements StatefulFunction {

  static final FunctionType TYPE = new FunctionType("ververica", "fn-with-dep");

  private final RuntimeDependency dependency;

  public FnWithDependency(RuntimeDependency dependency) {
    this.dependency = dependency;
  }

  @Override
  public void invoke(Context context, Object input) {}
}
