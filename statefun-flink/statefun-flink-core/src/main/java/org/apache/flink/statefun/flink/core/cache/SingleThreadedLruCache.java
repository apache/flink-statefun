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

package org.apache.flink.statefun.flink.core.cache;

import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.function.Function;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public final class SingleThreadedLruCache<K, V> {
  private final Cache<K, V> cache;

  public SingleThreadedLruCache(int maxCapacity) {
    this.cache = new Cache<>(maxCapacity, maxCapacity);
  }

  public void put(K key, V value) {
    cache.put(key, value);
  }

  @Nullable
  public V get(K key) {
    return cache.get(key);
  }

  public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
    return cache.computeIfAbsent(key, mappingFunction);
  }

  private static final class Cache<K, V> extends LinkedHashMap<K, V> {

    private static final long serialVersionUID = 1;

    private final int maxCapacity;

    private Cache(int initialCapacity, int maxCapacity) {
      super(initialCapacity, 0.75f, true);
      this.maxCapacity = maxCapacity;
    }

    @Override
    protected boolean removeEldestEntry(Entry<K, V> eldest) {
      return size() > maxCapacity;
    }
  }
}
