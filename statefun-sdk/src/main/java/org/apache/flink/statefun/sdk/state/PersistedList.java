package org.apache.flink.statefun.sdk.state;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.statefun.sdk.annotations.ForRuntime;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PersistedList<E> extends ManagedState {
    private static final Logger LOG = LoggerFactory.getLogger(PersistedList.class);
    private final String name;
    private final Class<E> elementType;
    private final Expiration expiration;
    protected NonFaultTolerantAccessor<E> cachingAccessor;
    protected ListAccessor<E> accessor;
    private final Boolean nonFaultTolerant;

    protected PersistedList(String name,
                            Class<E> elementType,
                            Expiration expiration,
                            ListAccessor<E> accessor,
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

    public Iterable<E> get(){ return cachingAccessor.get(); }

    public void add(@Nonnull E value){ cachingAccessor.add(value); }

    public void update(@Nonnull List<E> values){ cachingAccessor.update(values); }

    public void addAll(@Nonnull List<E> values){ cachingAccessor.addAll(values); }

    public E getIndex(int index) throws Exception { return cachingAccessor.getIndex(index); }

    public E pollFirst() throws Exception { return cachingAccessor.pollFirst(); }

    public E pollLast() throws Exception { return cachingAccessor.pollLast();}

    public Long size() throws Exception { return cachingAccessor.size(); }

    @Override
    public String toString() {
        return String.format(
                "PersistedList{name=%s, elementType=%s, expiration=%s}",
                name, elementType.getName(), expiration);
    }

    @ForRuntime
    void setAccessor(ListAccessor<E> newAccessor) {
        this.accessor = Objects.requireNonNull(newAccessor);
        this.cachingAccessor.initialize(this.accessor);
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
            this.accessor.update((List<E>) this.cachingAccessor.get());
            this.cachingAccessor.setActive(false);
        }
    }

    public static final class NonFaultTolerantAccessor<E> implements ListAccessor<E>, CachedAccessor {
        private List<E> list = new ArrayList<>();
        private ListAccessor<E> remoteAccesor;
        private boolean active;
        private boolean modified;

        public void initialize(ListAccessor<E> remote){
            remoteAccesor = remote;
            list = (List<E>) remoteAccesor.get();
            active = true;
            modified = false;
        }

        public boolean ifActive(){
            return active;
        }

        @Override
        public boolean ifModified() {
            return modified;
        }

        public void setActive(boolean active){
            this.active = active;
        }

        public void verifyValid(){
            if(!active){
                initialize(this.remoteAccesor);
            }
        }

        @Override
        public Iterable<E> get() {
            verifyValid();
            return list;
        }

        @Override
        public void add(@Nonnull E value) {
            verifyValid();
            list.add(value);
            modified = true;
        }

        @Override
        public void update(@Nonnull List<E> values) {
            verifyValid();
            list = values;
            modified = true;
        }

        @Override
        public void addAll(@Nonnull List<E> values) {
            verifyValid();
            list.addAll(values);
            modified = true;
        }

        @Override
        public E getIndex(int index) throws Exception {
            verifyValid();
            if(list.size() <= index){
                return null;
            }
            return list.get(index);
        }

        @Override
        public E pollFirst() throws Exception {
            verifyValid();
            modified = true;
            return list.remove(0);
        }

        @Override
        public E pollLast() throws Exception {
            verifyValid();
            modified = true;
            return list.remove(list.size() - 1);
        }

        @Override
        public void trim(int left, int right) throws Exception {
            verifyValid();
            modified = true;
            list = list.subList(left, right+1);
        }

        @Override
        public Long size() throws Exception {
            verifyValid();
            return (long)list.size();
        }
    }
}
