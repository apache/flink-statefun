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
package org.apache.flink.statefun.flink.core.translation;

import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.statefun.flink.common.TransientTypeInfo;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverse;
import org.apache.flink.statefun.flink.core.common.Maps;
import org.apache.flink.statefun.flink.core.types.StaticallyRegisteredTypes;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.util.OutputTag;

final class SideOutputTranslator {
  private final StatefulFunctionsUniverse universe;

  SideOutputTranslator(StatefulFunctionsUniverse universe) {
    this.universe = universe;
  }

  private static OutputTag<Object> outputTagFromId(
      EgressIdentifier<?> id, StaticallyRegisteredTypes types) {
    @SuppressWarnings("unchecked")
    EgressIdentifier<Object> casted = (EgressIdentifier<Object>) id;
    String name = String.format("%s.%s", id.namespace(), id.name());
    TypeInformation<Object> typeInformation = types.registerType(casted.consumedType());

    // The type information might refer to a class that only exists
    // on the module classloader. However, the type info field of the
    // output tag is only ever used to generate the stream graph
    // so we can safely mark it transient.
    return new OutputTag<>(name, new TransientTypeInfo<>(typeInformation));
  }

  Map<EgressIdentifier<?>, OutputTag<Object>> translate() {
    return Maps.transformValues(
        universe.egress(), (id, unused) -> outputTagFromId(id, universe.types()));
  }
}
