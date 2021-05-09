package org.apache.flink.statefun.sdk.state;

import org.apache.flink.statefun.sdk.annotations.ForRuntime;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class PersistedList<E> {
    private final String name;
    private final Class<E> elementType;
    private final Expiration expiration;
    private ListAccessor<E> accessor;
    private final Boolean nonFaultTolerant;

    private PersistedList(String name,
                          Class<E> elementType,
                          Expiration expiration,
                          ListAccessor<E> accessor,
                          Boolean nftFlag) {
        this.name = Objects.requireNonNull(name);
        this.elementType = Objects.requireNonNull(elementType);
        this.expiration = Objects.requireNonNull(expiration);
        this.accessor = Objects.requireNonNull(accessor);
        this.nonFaultTolerant = nftFlag;
    }

    public static <E> PersistedList<E> of(String name, Class<E> elementType) {
        return of(name, elementType, Expiration.none(), false);
    }

    public static <E> PersistedList<E> of(String name, Class<E> elementType, Boolean nftFlag) {
        return of(name, elementType, Expiration.none(), nftFlag);
    }

    public static <E> PersistedList<E> of(
            String name, Class<E> elementType, Expiration expiration, Boolean flag) {
        return new PersistedList<>(name, elementType, expiration, new NonFaultTolerantAccessor<>(), flag);
    }

    public String name() { return name; }

    public Class<E> elementType(){ return elementType; }

    public Expiration expiration() {
        return expiration;
    }

    public Iterable<E> get(){ return accessor.get(); }

    public void add(@Nonnull E value){ accessor.add(value); }

    public void update(@Nonnull List<E> values){ accessor.update(values); }

    public void addAll(@Nonnull List<E> values){ accessor.addAll(values); }

    public E getIndex(int index) throws Exception { return accessor.getIndex(index); }

    public E pollFirst() throws Exception { return accessor.pollFirst(); }

    public E pollLast() throws Exception { return accessor.pollLast();}

    public Long size() throws Exception { return accessor.size(); }

    @Override
    public String toString() {
        return String.format(
                "PersistedList{name=%s, elementType=%s, expiration=%s}",
                name, elementType.getName(), expiration);
    }

    @ForRuntime
    void setAccessor(ListAccessor<E> newAccessor) {
        if(this.nonFaultTolerant) return;
        this.accessor = Objects.requireNonNull(newAccessor);
    }

    private static final class NonFaultTolerantAccessor<E> implements ListAccessor<E> {
        private List<E> list = new ArrayList<>();

        @Override
        public Iterable<E> get() {
            return list;
        }

        @Override
        public void add(@Nonnull E value) {
            list.add(value);
        }

        @Override
        public void update(@Nonnull List<E> values) {
            list = values;
        }

        @Override
        public void addAll(@Nonnull List<E> values) {
            list.addAll(values);
        }

        @Override
        public E getIndex(int index) throws Exception {
            return list.get(index);
        }

        @Override
        public E pollFirst() throws Exception {
            return list.remove(0);
        }

        @Override
        public E pollLast() throws Exception {
            return list.remove(list.size() - 1);
        }

        @Override
        public void trim(int left, int right) throws Exception {
            list = list.subList(left, right+1);
        }

        @Override
        public Long size() throws Exception {
            return (long)list.size();
        }
    }
}
