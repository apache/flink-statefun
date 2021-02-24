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

package org.apache.flink.statefun.sdk.java.storage;

import static org.apache.flink.statefun.sdk.java.storage.StateValueContexts.StateValueContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import org.apache.flink.statefun.sdk.java.AddressScopedStorage;
import org.apache.flink.statefun.sdk.java.ValueSpec;
import org.apache.flink.statefun.sdk.java.annotations.Internal;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.slice.SliceProtobufUtil;
import org.apache.flink.statefun.sdk.java.types.TypeCharacteristics;
import org.apache.flink.statefun.sdk.java.types.TypeSerializer;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction.PersistedValueMutation;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.flink.statefun.sdk.shaded.com.google.protobuf.ByteString;

@Internal
public final class ConcurrentAddressScopedStorage implements AddressScopedStorage {

  private final List<Cell<?>> cells;

  public ConcurrentAddressScopedStorage(List<StateValueContext<?>> stateValues) {
    this.cells = createCells(stateValues);
  }

  @Override
  public <T> Optional<T> get(ValueSpec<T> valueSpec) {
    final Cell<T> cell = getCellOrThrow(valueSpec);
    return cell.get();
  }

  @Override
  public <T> void set(ValueSpec<T> valueSpec, T value) {
    final Cell<T> cell = getCellOrThrow(valueSpec);
    cell.set(value);
  }

  @Override
  public <T> void remove(ValueSpec<T> valueSpec) {
    final Cell<T> cell = getCellOrThrow(valueSpec);
    cell.remove();
  }

  @SuppressWarnings("unchecked")
  private <T> Cell<T> getCellOrThrow(ValueSpec<T> runtimeSpec) {
    // fast path: the user used the same ValueSpec reference to declare the function
    // and to index into the state.
    for (Cell<?> cell : cells) {
      ValueSpec<?> registeredSpec = cell.spec();
      if (runtimeSpec == registeredSpec) {
        return (Cell<T>) cell;
      }
    }
    return slowGetCellOrThrow(runtimeSpec);
  }

  @SuppressWarnings("unchecked")
  private <T> Cell<T> slowGetCellOrThrow(ValueSpec<T> valueSpec) {
    // unlikely slow path: when the users used a different ValueSpec instance in registration
    // and at runtime.
    for (Cell<?> cell : cells) {
      ValueSpec<?> thisSpec = cell.spec();
      String thisName = thisSpec.name();
      if (!thisName.equals(valueSpec.name())) {
        continue;
      }
      if (thisSpec.typeName().equals(valueSpec.typeName())) {
        return (Cell<T>) cell;
      }
      throw new IllegalStorageAccessException(
          valueSpec.name(),
          "Accessed state with incorrect type; state type was registered as "
              + thisSpec.typeName()
              + ", but was accessed as type "
              + valueSpec.typeName());
    }
    throw new IllegalStorageAccessException(
        valueSpec.name(), "State does not exist; make sure that this state was registered.");
  }

  public void addMutations(Consumer<PersistedValueMutation> consumer) {
    for (Cell<?> cell : cells) {
      cell.toProtocolValueMutation().ifPresent(consumer);
    }
  }

  // ===============================================================================
  //  Thread-safe state value cells
  // ===============================================================================

  private interface Cell<T> {
    Optional<T> get();

    void set(T value);

    void remove();

    Optional<FromFunction.PersistedValueMutation> toProtocolValueMutation();

    ValueSpec<T> spec();
  }

  private static <T> Optional<T> tryDeserialize(
      TypeSerializer<T> serializer, TypedValue typedValue) {
    if (!typedValue.getHasValue()) {
      return Optional.empty();
    }
    Slice slice = SliceProtobufUtil.asSlice(typedValue.getValue());
    T value = serializer.deserialize(slice);
    return Optional.ofNullable(value);
  }

  private static <T> ByteString serialize(TypeSerializer<T> serializer, T value) {
    Slice slice = serializer.serialize(value);
    return SliceProtobufUtil.asByteString(slice);
  }

  private static final class ImmutableTypeCell<T> implements Cell<T> {
    private final ReentrantLock lock = new ReentrantLock();
    private final ValueSpec<T> spec;
    private final TypedValue typedValue;
    private final TypeSerializer<T> serializer;

    private CellStatus status = CellStatus.UNMODIFIED;
    private T cachedObject;

    public ImmutableTypeCell(ValueSpec<T> spec, TypedValue typedValue) {
      this.spec = spec;
      this.typedValue = typedValue;
      this.serializer = Objects.requireNonNull(spec.type().typeSerializer());
    }

    @Override
    public Optional<T> get() {
      lock.lock();
      try {
        if (status == CellStatus.DELETED) {
          return Optional.empty();
        }
        if (cachedObject != null) {
          return Optional.of(cachedObject);
        }
        Optional<T> result = tryDeserialize(serializer, typedValue);
        result.ifPresent(object -> this.cachedObject = object);
        return result;
      } finally {
        lock.unlock();
      }
    }

    @Override
    public void set(T value) {
      if (value == null) {
        throw new IllegalStorageAccessException(
            spec.name(), "Can not set state to NULL. Please use remove() instead.");
      }
      lock.lock();
      try {
        cachedObject = value;
        status = CellStatus.MODIFIED;
      } finally {
        lock.unlock();
      }
    }

    @Override
    public void remove() {
      lock.lock();
      try {
        cachedObject = null;
        status = CellStatus.DELETED;
      } finally {
        lock.unlock();
      }
    }

    @Override
    public Optional<PersistedValueMutation> toProtocolValueMutation() {
      final String typeNameString = spec.typeName().asTypeNameString();
      switch (status) {
        case MODIFIED:
          final TypedValue.Builder newValue =
              TypedValue.newBuilder()
                  .setTypename(typeNameString)
                  .setHasValue(true)
                  .setValue(serialize(serializer, cachedObject));

          return Optional.of(
              PersistedValueMutation.newBuilder()
                  .setStateName(spec.name())
                  .setMutationType(PersistedValueMutation.MutationType.MODIFY)
                  .setStateValue(newValue)
                  .build());
        case DELETED:
          return Optional.of(
              PersistedValueMutation.newBuilder()
                  .setStateName(spec.name())
                  .setMutationType(PersistedValueMutation.MutationType.DELETE)
                  .build());
        case UNMODIFIED:
          return Optional.empty();
        default:
          throw new IllegalStateException("Unknown cell status: " + status);
      }
    }

    @Override
    public ValueSpec<T> spec() {
      return spec;
    }
  }

  private static final class MutableTypeCell<T> implements Cell<T> {
    private final ReentrantLock lock = new ReentrantLock();

    private final TypeSerializer<T> serializer;
    private final ValueSpec<T> spec;
    private TypedValue typedValue;
    private CellStatus status = CellStatus.UNMODIFIED;

    private MutableTypeCell(ValueSpec<T> spec, TypedValue typedValue) {
      this.spec = spec;
      this.typedValue = typedValue;
      this.serializer = Objects.requireNonNull(spec.type().typeSerializer());
    }

    @Override
    public Optional<T> get() {
      lock.lock();
      try {
        if (status == CellStatus.DELETED) {
          return Optional.empty();
        }
        return tryDeserialize(serializer, typedValue);
      } finally {
        lock.unlock();
      }
    }

    @Override
    public void set(T value) {
      if (value == null) {
        throw new IllegalStorageAccessException(
            spec.name(), "Can not set state to NULL. Please use remove() instead.");
      }
      lock.lock();
      try {
        final TypedValue newTypedValue =
            this.typedValue
                .toBuilder()
                .setHasValue(true)
                .setValue(serialize(serializer, value))
                .build();
        this.typedValue = newTypedValue;
        this.status = CellStatus.MODIFIED;
      } finally {
        lock.unlock();
      }
    }

    @Override
    public void remove() {
      lock.lock();
      try {
        status = CellStatus.DELETED;
      } finally {
        lock.unlock();
      }
    }

    @Override
    public Optional<PersistedValueMutation> toProtocolValueMutation() {
      switch (status) {
        case MODIFIED:
          return Optional.of(
              PersistedValueMutation.newBuilder()
                  .setStateName(spec.name())
                  .setMutationType(PersistedValueMutation.MutationType.MODIFY)
                  .setStateValue(typedValue)
                  .build());
        case DELETED:
          return Optional.of(
              PersistedValueMutation.newBuilder()
                  .setStateName(spec.name())
                  .setMutationType(PersistedValueMutation.MutationType.DELETE)
                  .build());
        case UNMODIFIED:
          return Optional.empty();
        default:
          throw new IllegalStateException("Unknown cell status: " + status);
      }
    }

    @Override
    public ValueSpec<T> spec() {
      return spec;
    }
  }

  private enum CellStatus {
    UNMODIFIED,
    MODIFIED,
    DELETED
  }

  private static List<Cell<?>> createCells(List<StateValueContext<?>> stateValues) {
    final List<Cell<?>> cells = new ArrayList<>(stateValues.size());

    for (StateValueContext<?> stateValueContext : stateValues) {
      final TypedValue typedValue = stateValueContext.protocolValue().getStateValue();
      final ValueSpec<?> spec = stateValueContext.spec();
      @SuppressWarnings({"unchecked", "rawtypes"})
      final Cell<?> cell =
          spec.type().typeCharacteristics().contains(TypeCharacteristics.IMMUTABLE_VALUES)
              ? new ImmutableTypeCell(spec, typedValue)
              : new MutableTypeCell(spec, typedValue);

      cells.add(cell);
    }

    return cells;
  }
}
