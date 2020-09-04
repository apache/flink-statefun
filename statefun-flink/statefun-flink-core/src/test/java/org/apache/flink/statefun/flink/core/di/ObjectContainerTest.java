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
package org.apache.flink.statefun.flink.core.di;

import static org.hamcrest.CoreMatchers.theInstance;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Test;

public class ObjectContainerTest {

  @Test
  public void addAliasTest() {
    final ObjectContainer container = new ObjectContainer();

    container.add("label-1", InterfaceA.class, TestClass.class);
    container.addAlias("label-2", InterfaceB.class, "label-1", InterfaceA.class);

    assertThat(
        container.get(InterfaceB.class, "label-2"),
        theInstance(container.get(InterfaceA.class, "label-1")));
  }

  private interface InterfaceA {}

  private interface InterfaceB {}

  private static class TestClass implements InterfaceA, InterfaceB {}
}
