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
package org.apache.flink.statefun.e2e.smoke.driver;

import static org.apache.flink.statefun.e2e.smoke.driver.Types.unpackSourceCommand;

import java.util.Objects;
import org.apache.flink.statefun.e2e.smoke.generated.SourceCommand;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.Router;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

public class CommandRouter implements Router<TypedValue> {
  private final Ids ids;

  public CommandRouter(Ids ids) {
    this.ids = Objects.requireNonNull(ids);
  }

  @Override
  public void route(TypedValue command, Downstream<TypedValue> downstream) {
    SourceCommand sourceCommand = unpackSourceCommand(command);
    FunctionType type = Constants.FN_TYPE;
    String id = ids.idOf(sourceCommand.getTarget());
    downstream.forward(type, id, command);
  }
}
