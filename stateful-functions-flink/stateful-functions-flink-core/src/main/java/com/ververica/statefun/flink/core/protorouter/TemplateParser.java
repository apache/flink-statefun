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
package com.ververica.statefun.flink.core.protorouter;

import static com.ververica.statefun.flink.core.protorouter.TemplateParser.TextFragment.dynamicFragment;
import static com.ververica.statefun.flink.core.protorouter.TemplateParser.TextFragment.staticFragment;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class TemplateParser {
  private static final Pattern DYNAMIC_FRAGMENT_PATTERN = Pattern.compile("\\{\\{([^}]*)}}");

  private TemplateParser() {}

  static List<TextFragment> parseTemplateString(String template) {
    ArrayList<TextFragment> fragments = new ArrayList<>();
    Matcher fragmentMatcher = DYNAMIC_FRAGMENT_PATTERN.matcher(template);
    int position = 0;
    while (position < template.length()) {
      if (!fragmentMatcher.find(position)) {
        // no more dynamic parts to the pattern. We take whatever we have left from position to the
        // end
        // as a static pattern, and terminate the loop.
        String prefix = template.substring(position);
        fragments.add(staticFragment(prefix));
        break;
      }
      // A dynamic text fragment has been found. It is of the form: X{{Y}}
      // where X is a static prefix that spans from (position, matchStart - 2)
      // and Y is the dynamic part that spans from (dynamicStart, dynamicEnd)
      final int prefixStart = position;
      final int prefixEnd = fragmentMatcher.start(1) - "{{".length();
      final int dynamicStart = fragmentMatcher.start(1);
      final int dynamicEnd = fragmentMatcher.end(1);
      if (prefixEnd - prefixStart > 0) {
        // we have a static prefix
        String prefixText = template.substring(prefixStart, prefixEnd);
        fragments.add(staticFragment(prefixText));
      }
      String dynamicFragmentText = template.substring(dynamicStart, dynamicEnd);
      fragments.add(dynamicFragment(dynamicFragmentText));
      position = dynamicEnd + "}}".length();
    }
    return fragments;
  }

  public static final class TextFragment {
    private final String fragment;
    private final boolean dynamic;

    public static TextFragment dynamicFragment(String text) {
      return new TextFragment(text, true);
    }

    public static TextFragment staticFragment(String text) {
      return new TextFragment(text, false);
    }

    private TextFragment(String fragment, boolean dynamic) {
      this.fragment = Objects.requireNonNull(fragment);
      this.dynamic = dynamic;
    }

    public String fragment() {
      return fragment;
    }

    public boolean dynamic() {
      return dynamic;
    }

    @Override
    public String toString() {
      return "Fragment{" + "text='" + fragment + '\'' + ", dynamic=" + dynamic + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TextFragment fragment = (TextFragment) o;
      return dynamic == fragment.dynamic && this.fragment.equals(fragment.fragment);
    }

    @Override
    public int hashCode() {
      return Objects.hash(fragment, dynamic);
    }
  }
}
