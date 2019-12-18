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
package com.ververica.statefun.flink.common.protopath;

import java.util.Objects;

final class PathFragment {
  private final String name;
  private final int index;

  PathFragment(String name, int index) {
    this.name = name;
    this.index = index;
  }

  PathFragment(String name) {
    this(name, -1);
  }

  boolean isRepeated() {
    return index >= 0;
  }

  public String getName() {
    return name;
  }

  int getIndex() {
    return index;
  }

  @Override
  public String toString() {
    return "ParsedField{" + "name='" + name + '\'' + ", index=" + index + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PathFragment that = (PathFragment) o;
    return index == that.index && name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, index);
  }
}
