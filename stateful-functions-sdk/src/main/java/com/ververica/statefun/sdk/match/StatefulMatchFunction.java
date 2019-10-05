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

package com.ververica.statefun.sdk.match;

import com.ververica.statefun.sdk.Context;
import com.ververica.statefun.sdk.StatefulFunction;

/**
 * A {@link StatefulMatchFunction} is an utility {@link StatefulFunction} that supports pattern
 * matching on function inputs to decide how the inputs should be processed.
 *
 * <p>Please see {@link MatchBinder} for the supported types of pattern matching.
 *
 * @see MatchBinder
 */
public abstract class StatefulMatchFunction implements StatefulFunction {

  private boolean setup = false;

  private MatchBinder matcher = new MatchBinder();

  /**
   * Configures the patterns to match for the function's inputs.
   *
   * @param binder a {@link MatchBinder} to bind patterns on.
   */
  public abstract void configure(MatchBinder binder);

  @Override
  public final void invoke(Context context, Object input) {
    ensureInitialized();
    matcher.invoke(context, input);
  }

  private void ensureInitialized() {
    if (!setup) {
      setup = true;
      configure(matcher);
    }
  }
}
