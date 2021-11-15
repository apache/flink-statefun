package org.apache.flink.statefun.sdk.state.mergeable;

import org.apache.flink.statefun.sdk.state.Accessor;
import org.apache.flink.statefun.sdk.state.Expiration;
import org.apache.flink.statefun.sdk.state.PersistedValue;

import java.util.function.BiFunction;

public class PersistedMergeableValue<T> extends PersistedValue<T> implements MergeableState {
    private BiFunction<T, T, T> mergingFunction;

    public PersistedMergeableValue(String name, Class type, Expiration expiration, Accessor accessor, Boolean nftFlag, BiFunction<T, T, T> func) {
        super(name, type, expiration, accessor, nftFlag);
        this.mergingFunction = func;
    }

    public static <T> PersistedMergeableValue<T> of(String name, Class<T> type, BiFunction<T, T, T> func){
        return new PersistedMergeableValue<T>(name, type, Expiration.none(), new NonFaultTolerantAccessor<>(), true, func);
    }

    @Override
    public void mergeRemote() {
        T val = accessor.get();
        val = mergingFunction.apply(cachingAccessor.get(), val);
        cachingAccessor.set(val);
        flush();
    }
}
