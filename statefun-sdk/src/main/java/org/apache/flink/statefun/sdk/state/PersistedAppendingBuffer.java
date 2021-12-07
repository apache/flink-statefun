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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.management.Descriptor;

import org.apache.flink.api.common.state.StateDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.ForRuntime;
import org.apache.flink.statefun.sdk.annotations.Persisted;

/**
 * A {@link PersistedAppendingBuffer} is an append-only buffer registered within {@link
 * StatefulFunction}s and is persisted and maintained by the system for fault-tolerance. Persisted
 * elements in the buffer may only be updated with bulk replacements.
 *
 * <p>Created persisted buffers must be registered by using the {@link Persisted} annotation. Please
 * see the class-level Javadoc of {@link StatefulFunction} for an example on how to do that.
 *
 * @see StatefulFunction
 * @param <E> type of the buffer elements.
 */
public final class PersistedAppendingBuffer<E> extends ManagedState{
  private static final Logger LOG = LoggerFactory.getLogger(PersistedAppendingBuffer.class);
  private final String name;
  private final Class<E> elementType;
  private final Expiration expiration;
  private NonFaultTolerantAccessor<E> cachingAccessor;
  private AppendingBufferAccessor<E> accessor;
  private final Boolean nonFaultTolerant;
  private StateDescriptor descriptor;

  private PersistedAppendingBuffer(
      String name,
      Class<E> elementType,
      Expiration expiration,
      AppendingBufferAccessor<E> accessor,
      Boolean nftFlag) {
    this.name = Objects.requireNonNull(name);
    this.elementType = Objects.requireNonNull(elementType);
    this.expiration = Objects.requireNonNull(expiration);
    if(!(cachingAccessor instanceof NonFaultTolerantAccessor)){
      LOG.error("cachingAccessor not of type NonFaultTolerantAccessor.");
    }
    this.cachingAccessor = (NonFaultTolerantAccessor<E>)Objects.requireNonNull(accessor);
    this.accessor = Objects.requireNonNull(accessor);
    this.nonFaultTolerant = Objects.requireNonNull(nftFlag);
    this.descriptor = null;
  }

  /**
   * Creates a {@link PersistedAppendingBuffer} instance that may be used to access persisted state
   * managed by the system. Access to the persisted buffer is identified by an unique name and type
   * of the elements. These may not change across multiple executions of the application.
   *
   * @param name the unique name of the persisted buffer state
   * @param elementType the type of the elements of this {@code PersistedAppendingBuffer}.
   * @param <E> the type of the elements.
   * @return a {@code PersistedAppendingBuffer} instance.
   */
  public static <E> PersistedAppendingBuffer<E> of(String name, Class<E> elementType) {
    return of(name, elementType, Expiration.none());
  }

  public static <E> PersistedAppendingBuffer<E> of(String name, Class<E> elementType, Boolean nonFaultTolerant) {
    return of(name, elementType, Expiration.none(), nonFaultTolerant);
  }

  /**
   * Creates a {@link PersistedAppendingBuffer} instance that may be used to access persisted state
   * managed by the system. Access to the persisted buffer is identified by an unique name and type
   * of the elements. These may not change across multiple executions of the application.
   *
   * @param name the unique name of the persisted buffer state
   * @param elementType the type of the elements of this {@code PersistedAppendingBuffer}.
   * @param expiration state expiration configuration.
   * @param <E> the type of the elements.
   * @return a {@code PersistedAppendingBuffer} instance.
   */
  public static <E> PersistedAppendingBuffer<E> of(
      String name, Class<E> elementType, Expiration expiration) {
    return new PersistedAppendingBuffer<>(
        name, elementType, expiration, new NonFaultTolerantAccessor<>(), false);
  }

  public static <E> PersistedAppendingBuffer<E> of(
          String name, Class<E> elementType, Expiration expiration, Boolean nftFlag) {
    return new PersistedAppendingBuffer<>(
            name, elementType, expiration, new NonFaultTolerantAccessor<>(), nftFlag);
  }

  /**
   * Returns the unique name of the persisted buffer.
   *
   * @return unique name of the persisted buffer.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the type of the persisted buffer elements.
   *
   * @return the type of the persisted buffer elements.
   */
  public Class<E> elementType() {
    return elementType;
  }

  public Expiration expiration() {
    return expiration;
  }

  /**
   * Appends an element to the persisted buffer.
   *
   * @param element the element to add to the persisted buffer.
   */
  public void append(@Nonnull E element) {
    cachingAccessor.append(element);
  }

  /**
   * Adds all elements of a list to the persisted buffer.
   *
   * <p>If an empty list is passed in, then this method has no effect and the persisted buffer
   * remains the same.
   *
   * @param elements a list of elements to add to the persisted buffer.
   */
  public void appendAll(@Nonnull List<E> elements) {
    if (!elements.isEmpty()) {
      cachingAccessor.appendAll(elements);
    }
  }

  /**
   * Replace the elements in the persisted buffer with the provided list of elements.
   *
   * <p>If an empty list is passed in, this method will have the same effect as {@link #clear()}.
   *
   * @param elements list of elements to replace the elements in the persisted buffer with.
   */
  public void replaceWith(@Nonnull List<E> elements) {
    if (!elements.isEmpty()) {
      cachingAccessor.replaceWith(elements);
    } else {
      cachingAccessor.clear();
    }
  }

  /**
   * Gets an unmodifiable view of the elements of the persisted buffer, as an {@link Iterable}.
   *
   * @return an unmodifiable view, as an {@link Iterable}, of the elements of the persisted buffer.
   */
  @Nonnull
  public Iterable<E> view() {
    return new UnmodifiableViewIterable<>(cachingAccessor.view());
  }

  /** Clears all elements in the persisted buffer. */
  public void clear() {
    cachingAccessor.clear();
  }

  @Override
  public String toString() {
    return String.format(
        "PersistedAppendingBuffer{name=%s, elementType=%s, expiration=%s}",
        name, elementType.getName(), expiration);
  }

  @ForRuntime
  void setAccessor(AppendingBufferAccessor<E> newAccessor) {
    this.accessor = Objects.requireNonNull(newAccessor);
    this.cachingAccessor.initialize(this.accessor);
  }

  public void setDescriptor(StateDescriptor descriptor){
    this.descriptor = descriptor;
  }

  @Override
  public Boolean ifNonFaultTolerance() {
      return nonFaultTolerant;
  }

  @Override
  public void setInactive() {
    this.cachingAccessor.setActive(false);
  }

  @Override
  public void flush() {
    if(this.cachingAccessor.ifActive()){
      this.accessor.replaceWith((List<E>) this.cachingAccessor.view());
      this.cachingAccessor.setActive(false);
    }
  }

  @Override
  public StateDescriptor getDescriptor() {
    return this.descriptor;
  }

  private static final class NonFaultTolerantAccessor<E> implements AppendingBufferAccessor<E>, CachedAccessor {
    private List<E> list = new ArrayList<>();
    private AppendingBufferAccessor<E> remoteAccessor;
    private boolean active;
    private boolean modified;

    public void initialize(AppendingBufferAccessor<E> remote){
      remoteAccessor = remote;
      list = (List<E>) remoteAccessor.view();
      active = true;
      modified = false;
    }

    @Override
    public void append(@Nonnull E element) {
      verifyValid();
      list.add(element);
      modified = true;
    }

    @Override
    public void appendAll(@Nonnull List<E> elements) {
      verifyValid();
      list.addAll(elements);
      modified = true;
    }

    @Override
    public void replaceWith(@Nonnull List<E> elements) {
      verifyValid();
      list.clear();
      list.addAll(elements);
      modified = true;
    }

    @Nonnull
    @Override
    public Iterable<E> view() {
      verifyValid();
      return list;
    }

    @Override
    public void clear() {
      list.clear();
      modified = true;
    }

    @Override
    public boolean ifActive() {
      return active;
    }

    @Override
    public boolean ifModified() {
      return modified;
    }

    @Override
    public void setActive(boolean active) {
      this.active = active;
    }

    @Override
    public void verifyValid() {
      if(!active){
        initialize(this.remoteAccessor);
      }
    }
  }

  private static final class UnmodifiableViewIterable<E> implements Iterable<E> {

    private final Iterable<E> delegate;

    private UnmodifiableViewIterable(Iterable<E> delegate) {
      this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public Iterator<E> iterator() {
      return new UnmodifiableViewIterator<>(delegate.iterator());
    }
  }

  private static final class UnmodifiableViewIterator<E> implements Iterator<E> {

    private final Iterator<E> delegate;

    UnmodifiableViewIterator(Iterator<E> delegate) {
      this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public boolean hasNext() {
      return delegate.hasNext();
    }

    @Override
    public E next() {
      return delegate.next();
    }

    public final void remove() {
      throw new UnsupportedOperationException("This is an unmodifiable view.");
    }
  }
}