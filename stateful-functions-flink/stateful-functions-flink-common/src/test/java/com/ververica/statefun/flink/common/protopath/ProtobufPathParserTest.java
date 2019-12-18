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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import java.util.List;
import org.junit.Test;

public class ProtobufPathParserTest {

  @Test
  public void exampleUsage() {
    List<PathFragment> fragments = ProtobufPathParser.parse("$.foo.bar.baz");

    assertThat(fragments, contains(fragment("foo"), fragment("bar"), fragment("baz")));
  }

  @Test
  public void repeatedField() {
    List<PathFragment> fragments = ProtobufPathParser.parse("$.foo[1].bar[7]");

    assertThat(fragments, contains(fragment("foo", 1), fragment("bar", 7)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void noPrefix() {
    ProtobufPathParser.parse("foo");
  }

  @Test(expected = IllegalArgumentException.class)
  public void badIndex() {
    ProtobufPathParser.parse("$.foo[-1]");
  }

  @Test(expected = IllegalArgumentException.class)
  public void badSyntaxWithIndexing() {
    ProtobufPathParser.parse("$.foo[[1]");
  }

  @Test(expected = IllegalArgumentException.class)
  public void badSyntaxWithMultipleDots() {
    ProtobufPathParser.parse("$..foo..bar");
  }

  private static PathFragment fragment(String name) {
    return new PathFragment(name);
  }

  private static PathFragment fragment(String name, int index) {
    return new PathFragment(name, index);
  }
}
