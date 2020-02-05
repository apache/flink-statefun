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
package org.apache.flink.statefun.sdk.match;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import org.apache.flink.statefun.sdk.Context;

/**
 * Binds patterns to be matched on inputs and their corresponding actions to the processing logic of
 * a {@link StatefulMatchFunction}.
 *
 * <p>The following methods are supported for binding patterns, in order of precedence that they are
 * checked for matches:
 *
 * <ul>
 *   <li>{@link #predicate(Class, Predicate, BiConsumer)}: matches on the input's type and a
 *       conditional predicate on the input object state.
 *   <li>{@link #predicate(Class, BiConsumer)}: simple type match on the input's type.
 *   <li>{@link #otherwise(BiConsumer)}: default action to take if no match can be found for the
 *       input.
 * </ul>
 */
public final class MatchBinder {

  private final IdentityHashMap<Class<?>, List<Method<Object>>> multimethods =
      new IdentityHashMap<>();

  private final IdentityHashMap<Class<?>, BiConsumer<Context, Object>> noPredicateMethods =
      new IdentityHashMap<>();

  private boolean customDefaultAction = false;

  private BiConsumer<Context, Object> defaultAction = MatchBinder::unhandledDefaultAction;

  MatchBinder() {}

  /**
   * Binds a simple type pattern which matches on the input's type.
   *
   * <p>This has a lower precedence than matches found on patterns registered via {@link
   * #predicate(Class, Predicate, BiConsumer)}. If no conditional predicates matches for a given
   * input of type {@code type}, then the action registered here will be used.
   *
   * @param type the expected input type.
   * @param action the action to take if this pattern matches.
   * @param <T> the expected input type.
   * @return the binder, with predicate bound
   */
  @SuppressWarnings("unchecked")
  public <T> MatchBinder predicate(Class<T> type, BiConsumer<Context, T> action) {
    Objects.requireNonNull(type);
    Objects.requireNonNull(action);

    if (noPredicateMethods.containsKey(type)) {
      throw new IllegalStateException("There is already a catch all case for class " + type);
    }

    noPredicateMethods.put(type, (BiConsumer<Context, Object>) action);
    return this;
  }

  /**
   * Binds a pattern which matches on a function's input type, as well as a conditional predicate on
   * the input object's state.
   *
   * <p>Precedence of conditional predicate matches is determined by the order in which they were
   * bind; predicates that were bind first have higher precedence. Patterns bind via this method
   * have the highest precedence over other methods.
   *
   * @param type the expected input type.
   * @param predicate a predicate on the input object state to match on.
   * @param action the action to take if this patten matches.
   * @param <T> the expected input type.
   * @return the binder, with predicate bound
   */
  @SuppressWarnings("unchecked")
  public <T> MatchBinder predicate(
      Class<T> type, Predicate<T> predicate, BiConsumer<Context, T> action) {
    List<Method<Object>> methods = multimethods.computeIfAbsent(type, ignored -> new ArrayList<>());
    BiConsumer<Context, Object> a = (BiConsumer<Context, Object>) action;
    Predicate<Object> p = (Predicate<Object>) predicate;
    methods.add(new Method<>(p, a));
    return this;
  }

  /**
   * Binds a default action for inputs that fail to match any of the patterns bind via the {@link
   * #predicate(Class, Predicate, BiConsumer)} and {@link #predicate(Class, BiConsumer)} methods. If
   * no default action was bind using this method, then a {@link IllegalStateException} would be
   * thrown for inputs that fail to match.
   *
   * @param action the default action
   * @return the binder, with default action bound
   */
  public MatchBinder otherwise(BiConsumer<Context, Object> action) {
    if (customDefaultAction) {
      throw new IllegalStateException("There can only be one default action");
    }

    customDefaultAction = true;
    defaultAction = Objects.requireNonNull(action);
    return this;
  }

  void invoke(Context context, Object input) {
    final Class<?> type = input.getClass();

    List<Method<Object>> methods = multimethods.getOrDefault(type, Collections.emptyList());
    for (Method<Object> m : methods) {
      if (m.canApply(input)) {
        m.apply(context, input);
        return;
      }
    }

    noPredicateMethods.getOrDefault(type, defaultAction).accept(context, input);
  }

  private static final class Method<T> {
    private final Predicate<T> predicate;
    private final BiConsumer<Context, T> apply;

    Method(Predicate<T> predicate, BiConsumer<Context, T> method) {
      this.predicate = Objects.requireNonNull(predicate);
      this.apply = Objects.requireNonNull(method);
    }

    boolean canApply(T input) {
      return predicate.test(input);
    }

    void apply(Context context, T input) {
      apply.accept(context, input);
    }
  }

  private static void unhandledDefaultAction(Context context, Object input) {
    throw new IllegalStateException("Don't know how to handle " + input);
  }
}
