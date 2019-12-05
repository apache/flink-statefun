package com.ververica.statefun.flink.core.protorouter;

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

import static com.ververica.statefun.flink.core.protorouter.TemplateParser.TextFragment.staticFragment;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

import com.ververica.statefun.flink.core.protorouter.TemplateParser.TextFragment;
import java.util.List;
import org.junit.Test;

public class TemplateParserTest {

  @Test
  public void exampleUsage() {
    List<TextFragment> fragments = TemplateParser.parseTemplateString("hello-{{world}}");

    assertThat(
        fragments, contains(staticFragment("hello-"), TextFragment.dynamicFragment("world")));
  }

  @Test
  public void anotherExample() {
    List<TextFragment> fragments =
        TemplateParser.parseTemplateString("io.example/greet-python/{{$.who[0].what[5].name}}");

    assertThat(
        fragments,
        contains(
            staticFragment("io.example/greet-python/"),
            TextFragment.dynamicFragment("$.who[0].what[5].name")));
  }

  @Test
  public void longDynamicText() {
    List<TextFragment> fragments =
        TemplateParser.parseTemplateString("{{this text should be dynamic}}");

    assertThat(fragments, contains(TextFragment.dynamicFragment("this text should be dynamic")));
  }

  @Test
  public void twoDynamicFragmentsWithSeparator() {
    List<TextFragment> fragments = TemplateParser.parseTemplateString("{{hello}}/{{world}}");

    assertThat(
        fragments,
        contains(
            TextFragment.dynamicFragment("hello"),
            staticFragment("/"),
            TextFragment.dynamicFragment("world")));
  }

  @Test
  public void twoDynamicFragmentsWithoutSeparator() {
    List<TextFragment> fragments = TemplateParser.parseTemplateString("{{hello}}{{world}}");

    assertThat(
        fragments,
        contains(TextFragment.dynamicFragment("hello"), TextFragment.dynamicFragment("world")));
  }

  @Test
  public void noDynamicText() {
    List<TextFragment> fragments = TemplateParser.parseTemplateString("hello world");

    assertThat(fragments, contains(staticFragment("hello world")));
  }

  @Test
  public void dynamicFragmentToTheLeft() {
    List<TextFragment> fragments = TemplateParser.parseTemplateString("{{hello}}-world");

    assertThat(
        fragments, contains(TextFragment.dynamicFragment("hello"), staticFragment("-world")));
  }

  @Test
  public void unclosedInterpolatedTextConsideredASticFragment() {
    assertThat(TemplateParser.parseTemplateString("{{"), contains(staticFragment("{{")));
  }
}
