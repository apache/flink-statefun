package org.apache.flink.statefun.sdk.state.mergeable;

import org.apache.flink.statefun.sdk.state.Expiration;
import org.apache.flink.statefun.sdk.state.ListAccessor;
import org.apache.flink.statefun.sdk.state.PersistedList;

import java.util.List;
import java.util.function.BiFunction;

public class PersistedMergeableList<T> extends PersistedList<T> implements MergeableState {
    private BiFunction<Iterable<T>, Iterable<T>, Iterable<T>> mergingFunction;

    public PersistedMergeableList(String name, Class type, Expiration expiration, ListAccessor<T> accessor, Boolean nftFlag, BiFunction<Iterable<T>, Iterable<T>, Iterable<T>> func){
        super(name, type, expiration, accessor, nftFlag);
        this.mergingFunction = func;
    }

    public static <T> PersistedMergeableList<T> of(String name, Class<T> type, BiFunction<Iterable<T>, Iterable<T>, Iterable<T>> func){
        return new PersistedMergeableList<T>(name, type, Expiration.none(), new NonFaultTolerantAccessor<>(), true, func);
    }

    @Override
    public void mergeRemote() {
        Iterable<T> list = accessor.get();
        list = mergingFunction.apply(cachingAccessor.get(), list);
        cachingAccessor.update((List<T>) list);
        flush();
    }
}
