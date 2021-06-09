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
package org.apache.flink.statefun.e2e.smoke;

import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.message.Message;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class CommandInterpreterFn implements StatefulFunction {

  static final TypeName TYPENAME = TypeName.typeNameOf("statefun.e2e", "interpreter");
  static final ValueSpec<Long> state = ValueSpec.named("state").withLongType();

  final CommandInterpreter interpreter;
  final StatefulFunctionSpec SPEC;

  // constructor and apply impl. are binded to command interpreter, should be something like InterpreterFn
  public CommandInterpreterFn(CommandInterpreter interpreter) {
    this.interpreter = Objects.requireNonNull(interpreter);
    this.SPEC =
        StatefulFunctionSpec.builder(TYPENAME)
            .withSupplier(() -> new CommandInterpreterFn(interpreter))
            .build();
  }

  @Override
  public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
    interpreter.interpret(state, context, message);
    return context.done();
  }
}
