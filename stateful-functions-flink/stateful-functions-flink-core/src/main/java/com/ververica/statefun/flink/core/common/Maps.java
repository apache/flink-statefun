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

package com.ververica.statefun.flink.core.common;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

public final class Maps {
  private Maps() {}

  public static <K, V, U> Map<K, U> transformValues(Map<K, V> map, Function<V, U> fn) {
    Map<K, U> result = new HashMap<>();

    for (Map.Entry<K, V> entry : map.entrySet()) {
      U u = fn.apply(entry.getValue());
      result.put(entry.getKey(), u);
    }

    return result;
  }

  public static <K, V, U> Map<K, U> transformValues(Map<K, V> map, BiFunction<K, V, U> fn) {
    Map<K, U> result = new HashMap<>();

    for (Map.Entry<K, V> entry : map.entrySet()) {
      U u = fn.apply(entry.getKey(), entry.getValue());
      result.put(entry.getKey(), u);
    }

    return result;
  }

  public static <K, T> Map<K, T> index(Iterable<T> elements, Function<T, K> indexBy) {
    Map<K, T> index = new HashMap<>();
    for (T element : elements) {
      index.put(indexBy.apply(element), element);
    }
    return index;
  }
}
