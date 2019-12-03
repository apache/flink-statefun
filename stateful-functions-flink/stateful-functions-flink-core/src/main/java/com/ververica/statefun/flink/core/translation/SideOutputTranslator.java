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

package com.ververica.statefun.flink.core.translation;

import com.ververica.statefun.flink.core.StatefulFunctionsUniverse;
import com.ververica.statefun.flink.core.common.Maps;
import com.ververica.statefun.flink.core.types.StaticallyRegisteredTypes;
import com.ververica.statefun.sdk.io.EgressIdentifier;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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
    return new OutputTag<>(name, typeInformation);
  }

  Map<EgressIdentifier<?>, OutputTag<Object>> translate() {
    return Maps.transformValues(
        universe.egress(), (id, unused) -> outputTagFromId(id, universe.types()));
  }
}
