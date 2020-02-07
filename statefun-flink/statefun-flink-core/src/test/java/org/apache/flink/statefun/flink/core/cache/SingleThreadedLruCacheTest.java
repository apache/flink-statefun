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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class SingleThreadedLruCacheTest {

  @Test
  public void exampleUsage() {
    SingleThreadedLruCache<String, String> cache = new SingleThreadedLruCache<>(2);

    cache.put("a", "1");
    cache.put("b", "2");

    assertThat(cache.get("a"), is("1"));
    assertThat(cache.get("b"), is("2"));
  }

  @Test
  public void leastRecentlyElementShouldBeEvicted() {
    SingleThreadedLruCache<String, String> cache = new SingleThreadedLruCache<>(2);

    cache.put("a", "1");
    cache.put("b", "2");
    cache.put("c", "3");

    assertThat(cache.get("a"), nullValue());
    assertThat(cache.get("b"), is("2"));
    assertThat(cache.get("c"), is("3"));
  }
}
