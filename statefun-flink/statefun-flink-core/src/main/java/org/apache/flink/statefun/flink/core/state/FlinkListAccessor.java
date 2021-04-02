package org.apache.flink.statefun.flink.core.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.statefun.sdk.state.ListAccessor;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;

public class FlinkListAccessor<E> implements ListAccessor<E> {

    private final ListState<E> handle;

    FlinkListAccessor(ListState<E> handle){
        System.out.println("FlinkListAccessor add handle " + handle.toString());
        this.handle = Objects.requireNonNull(handle);
    }

    @Override
    public Iterable<E> get() {
        try {
            Iterable<E> ret = handle.get();
            //System.out.println("FlinkListAccessor get iterable " + ret);
            return ret;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void add(@Nonnull E value) {
        try {
            //System.out.println("FlinkListAccessor add element " + value);
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
