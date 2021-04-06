package org.apache.flink.statefun.flink.core.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.statefun.sdk.state.ListAccessor;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;

public class FlinkListAccessor<E> implements ListAccessor<E> {

    private final ListState<E> handle;

    FlinkListAccessor(ListState<E> handle){
        this.handle = Objects.requireNonNull(handle);
    }

    @Override
    public Iterable<E> get() {
        try {
            Iterable<E> ret = handle.get();
            return ret;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void add(@Nonnull E value) {
        try {
            handle.add(value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void update(@Nonnull List<E> values) {
        try {
            handle.update(values);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addAll(@Nonnull List<E> values) {
        try {
            handle.addAll(values);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
