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

package com.ververica.statefun.examples.ridesharing.simulator.simulation;

import static com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

final class StateMachine<S extends Enum<S>> {

  public interface Behaviour<S extends Enum<S>, E> {
    S apply(E event);
  }

  private final class Case<E> {
    private final Class<E> eventType;
    private final Predicate<E> predicate;
    private final Behaviour<S, E> action;

    Case(Class<E> eventType, Predicate<E> predicate, Behaviour<S, E> action) {
      this.eventType = eventType;
      this.predicate = predicate;
      this.action = action;
    }

    Optional<S> tryApply(Object event) {
      if (!eventType.isInstance(event)) {
        return Optional.empty();
      }
      final E e = eventType.cast(event);
      if (!predicate.test(e)) {
        return Optional.empty();
      }
      final S nextState = action.apply(e);
      return Optional.ofNullable(nextState);
    }
  }

  private final Map<S, List<Case<?>>> cases = new HashMap<>();

  private S current;
  private final Set<S> terminalStates = new HashSet<>();

  StateMachine(S initialState) {
    this.current = initialState;
  }

  @SuppressWarnings("UnusedReturnValue")
  <E> StateMachine<S> withState(S state, Class<E> eventType, Behaviour<S, E> action) {
    return withState(state, eventType, unused -> true, action);
  }

  @SuppressWarnings("UnusedReturnValue")
  <E> StateMachine<S> withState(
      S state, Class<E> eventType, Predicate<E> guard, Behaviour<S, E> action) {
    List<Case<?>> stateCases = cases.computeIfAbsent(state, unused -> new ArrayList<>());
    stateCases.add(new Case<>(eventType, guard, action));
    return this;
  }

  void withTerminalState(S terminalState) {
    this.terminalStates.add(terminalState);
  }

  void apply(Object event) {
    checkState(!terminalStates.contains(current), "Already at a terminal state " + current);
    Optional<S> next = tryApply(event);
    if (!next.isPresent()) {
      throw new IllegalArgumentException(
          "Don't know how to handle the event "
              + safeGetClass(event)
              + " in state "
              + current
              + " event:"
              + event);
    }
    current = next.get();
  }

  boolean isAtTerminalState() {
    return terminalStates.contains(current);
  }

  private Optional<S> tryApply(Object event) {
    final List<Case<?>> stateCases = cases.getOrDefault(current, Collections.emptyList());
    for (Case<?> c : stateCases) {
      final Optional<S> nextState = c.tryApply(event);
      if (nextState.isPresent()) {
        return nextState;
      }
    }
    return Optional.empty();
  }

  private static String safeGetClass(Object event) {
    return event == null ? "<null>" : event.getClass().toString();
  }
}
