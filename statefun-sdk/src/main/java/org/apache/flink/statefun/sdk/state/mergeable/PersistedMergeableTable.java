package org.apache.flink.statefun.sdk.state.mergeable;

import org.apache.flink.statefun.sdk.state.Expiration;
import org.apache.flink.statefun.sdk.state.PersistedTable;
import org.apache.flink.statefun.sdk.state.TableAccessor;

import java.util.Map;
import java.util.function.BiFunction;

public class PersistedMergeableTable<K, V> extends PersistedTable<K, V> implements MergeableState {
    private BiFunction<Iterable<Map.Entry<K, V>>, Iterable<Map.Entry<K, V>>, Iterable<Map.Entry<K, V>>> mergingFunction;

    public PersistedMergeableTable(String name, Class<K> keyType, Class<V> valueType, Expiration expiration, TableAccessor<K, V> accessor, Boolean nftFlag, BiFunction<Iterable<Map.Entry<K, V>>, Iterable<Map.Entry<K, V>>, Iterable<Map.Entry<K, V>>> func){
        super(name, keyType, valueType, expiration, accessor, nftFlag);
        mergingFunction = func;
    }

    public static <K, V> PersistedMergeableTable<K, V> of(String name, Class<K> keyType, Class<V> valueType,BiFunction<Iterable<Map.Entry<K, V>>, Iterable<Map.Entry<K, V>>, Iterable<Map.Entry<K, V>>> func){
        return new PersistedMergeableTable<K, V>(name, keyType, valueType, Expiration.none(), new NonFaultTolerantAccessor<>(), true, func);
    }

    @Override
    public void mergeRemote() {
        Iterable<Map.Entry<K, V>> map = accessor.entries();
        map = mergingFunction.apply(cachingAccessor.entries(), map);
        cachingAccessor.clear();
        // TODO: need a batch writer for map
        for(Map.Entry<K, V> e : map){
            cachingAccessor.set(e.getKey(), e.getValue());
        }
    }
}
