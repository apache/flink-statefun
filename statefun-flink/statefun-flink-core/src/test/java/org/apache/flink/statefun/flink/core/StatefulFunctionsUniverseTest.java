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

package org.apache.flink.statefun.flink.core;

import static org.hamcrest.core.IsSame.sameInstance;
import static org.junit.Assert.assertThat;

import org.apache.flink.statefun.flink.core.message.MessageFactoryKey;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;
import org.apache.flink.statefun.sdk.TypeName;
import org.junit.Test;

public class StatefulFunctionsUniverseTest {

  @Test
  public void testExtensions() {
    final StatefulFunctionsUniverse universe = emptyUniverse();

    final ExtensionImpl extension = new ExtensionImpl();
    universe.bindExtension(TypeName.parseFrom("test.namespace/test.name"), extension);

    assertThat(
        extension,
        sameInstance(
            universe.resolveExtension(
                TypeName.parseFrom("test.namespace/test.name"), BaseExtension.class)));
  }

  private static StatefulFunctionsUniverse emptyUniverse() {
    return new StatefulFunctionsUniverse(
        MessageFactoryKey.forType(MessageFactoryType.WITH_PROTOBUF_PAYLOADS, null));
  }

  private interface BaseExtension {}

  private static class ExtensionImpl implements BaseExtension {}
}
