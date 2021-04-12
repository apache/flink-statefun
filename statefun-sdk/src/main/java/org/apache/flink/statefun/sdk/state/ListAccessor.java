package org.apache.flink.statefun.sdk.state;

import javax.annotation.Nonnull;
import java.util.List;

public interface ListAccessor<E> {

    Iterable<E> get();

    void add(@Nonnull E value);

    void update(@Nonnull List<E> values);

    void addAll(@Nonnull List<E> values);

    public E getIndex(int index) throws Exception;

    public E pollFirst() throws Exception;

    public E pollLast() throws Exception;
}
