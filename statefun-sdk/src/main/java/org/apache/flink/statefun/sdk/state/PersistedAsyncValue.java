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
package org.apache.flink.statefun.sdk.state;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.ForRuntime;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * A {@link PersistedValue} is a value registered within {@link StatefulFunction}s and is persisted
 * and maintained by the system for fault-tolerance.
 *
 * <p>Created persisted values must be registered by using the {@link Persisted} annotation. Please
 * see the class-level Javadoc of {@link StatefulFunction} for an example on how to do that.
 *
 * @see StatefulFunction
 * @param <T> type of the state.
 */
public class PersistedAsyncValue<T> extends ManagedState {
    private final String name;
    private final Class<T> type;
    private final Expiration expiration;
    protected AsyncAccessor<T> accessor;
    private final Boolean nonFaultTolerant;
    private StateDescriptor descriptor;

    protected PersistedAsyncValue(String name, Class<T> type, Expiration expiration, AsyncAccessor<T> accessor, Boolean nftFlag) {
        this.name = Objects.requireNonNull(name);
        this.type = Objects.requireNonNull(type);
        this.expiration = Objects.requireNonNull(expiration);
        this.accessor = Objects.requireNonNull(accessor);
        this.nonFaultTolerant = Objects.requireNonNull(nftFlag);
        this.descriptor = null;
    }

    /**
     * Creates a {@link PersistedAsyncValue} instance that may be used to access persisted state managed by
     * the system. Access to the persisted value is identified by an unique name and type of the
     * value. These may not change across multiple executions of the application.
     *
     * @param name the unique name of the persisted state.
     * @param type the type of the state values of this {@code PersistedValue}.
     * @param <T> the type of the state values.
     * @return a {@code PersistedValue} instance.
     */
    public static <T> PersistedAsyncValue<T> of(String name, Class<T> type) {
        return of(name, type, Expiration.none());
    }

    public static <T> PersistedAsyncValue<T> of(String name, Class<T> type, Boolean nonFaultTolerant) {
        return of(name, type, Expiration.none(), nonFaultTolerant);
    }

    /**
     * Creates a {@link PersistedValue} instance that may be used to access persisted state managed by
     * the system. Access to the persisted value is identified by an unique name and type of the
     * value. These may not change across multiple executions of the application.
     *
     * @param name the unique name of the persisted state.
     * @param type the type of the state values of this {@code PersistedValue}.
     * @param expiration state expiration configuration.
     * @param <T> the type of the state values.
     * @return a {@code PersistedValue} instance.
     */
    public static <T> PersistedAsyncValue<T> of(String name, Class<T> type, Expiration expiration) {
        return new PersistedAsyncValue<T>(name, type, expiration, new NonFaultTolerantAccessor<T>(), false);
    }

    public static <T> PersistedAsyncValue<T> of(String name, Class<T> type, Expiration expiration, Boolean nftFlag) {
        return new PersistedAsyncValue<T>(name, type, expiration, new NonFaultTolerantAccessor<T>(), nftFlag);
    }

    /**
     * Returns the unique name of the persisted value.
     *
     * @return unique name of the persisted value.
     */
    public String name() {
        return name;
    }

    /**
     * Returns the type of the persisted values.
     *
     * @return the type of the persisted values.
     */
    public Class<T> type() {
        return type;
    }

    public Expiration expiration() {
        return expiration;
    }

    /**
     * Returns the persisted value.
     *
     * @return the persisted value.
     */
    public T get() {
        try {
            return accessor.getAsync().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Updates the persisted value.
     *
     * @param value the new value.
     */
    public void set(T value) {
        try {
            accessor.setAsync(value).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the persisted value.
     *
     * @return the persisted value.
     */
    public CompletableFuture<T> getAsync() {
        return accessor.getAsync();
    }

    /**
     * Updates the persisted value.
     *
     * @param value the new value.
     */
    public CompletableFuture<String> setAsync(T value) {
        return accessor.setAsync(value);
    }

    /** Clears the persisted value. After being cleared, the value would be {@code null}. */
    public void clear() {
        accessor.clear();
    }

    /**
     * Updates the persisted value and returns it, in a single operation.
     *
     * @param update function to process the previous value to obtain the new value.
     * @return the new updated value.
     */
    public T updateAndGet(Function<T, T> update) {
        throw new NotImplementedException();
    }


    public CompletableFuture<T> updateAndGetAsync(Function<T, T> update) {
        T current = null;
        CompletableFuture<T> ret = CompletableFuture.completedFuture(null);
        try {
            CompletableFuture<T> future = accessor.getAsync();
            current = future.get();
            T updated = update.apply(current);
            ret = accessor.setAsync(updated).thenApply(x->updated);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return ret;
    }

    /**
     * Attempts to get the persisted value. If the current value is {@code null}, then a specified
     * default is returned instead.
     *
     * @param orElse the default value to return if the current value is not present.
     * @return the persisted value, or the provided default if it isn't present.
     */
    public T getOrDefault(T orElse) {
        throw new NotImplementedException();
    }

    /**
     * Attempts to get the persisted value. If the current value is {@code null}, then a default value
     * obtained from a specified supplier is returned instead.
     *
     * @param defaultSupplier supplier for a default value to be used if the current value is not
     *     present.
     * @return the persisted value, or a default value if it isn't present.
     */
    public T getOrDefault(Supplier<T> defaultSupplier) {
        throw new NotImplementedException();
    }

    @ForRuntime
    void setAccessor(AsyncAccessor<T> newAccessor) {
        if(this.nonFaultTolerant) return;
        Objects.requireNonNull(newAccessor);
        this.accessor = newAccessor;
    }

    public void setDescriptor(StateDescriptor descriptor){
        this.descriptor = descriptor;
    }

    @Override
    public String toString() {
        return String.format(
                "PersistedAsyncValue{name=%s, type=%s, expiration=%s}", name, type.getName(), expiration);
    }

    @Override
    public Boolean ifNonFaultTolerance() {
        return nonFaultTolerant;
    }

    @Override
    public void setInactive() {
        throw new NotImplementedException();
    }

    @Override
    public void flush() {
        throw new NotImplementedException();
    }

    @Override
    public StateDescriptor getDescriptor() {
        return descriptor;
    }

    private static final class NonFaultTolerantAccessor<E> implements AsyncAccessor<E> {
        private E element;

        @Override
        public CompletableFuture<String> setAsync(E element) {
            this.element = element;
            return CompletableFuture.completedFuture("");
        }

        @Override
        public CompletableFuture<E> getAsync() {
            return CompletableFuture.completedFuture(element);
        }

        @Override
        public void set(E value) {
            throw new NotImplementedException();
        }

        @Override
        public E get() {
            throw new NotImplementedException();
        }

        @Override
        public void clear() {
            element = null;
        }
    }
}
