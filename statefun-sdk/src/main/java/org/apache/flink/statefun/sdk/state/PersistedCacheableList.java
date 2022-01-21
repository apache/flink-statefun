package org.apache.flink.statefun.sdk.state;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.statefun.sdk.annotations.ForRuntime;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


public final class PersistedCacheableList<E> extends CacheableState{
    private final String name;
    private final Class<E> elementType;
    private final Expiration expiration;
    private boolean synced;
    private ListAccessor<E> accessor;
    protected NonFaultTolerantAccessor<E> cachedAccessor;
    private int lDelta;
    private int rDelta;
    private boolean valueCached;
    private StateDescriptor descriptor;

    private PersistedCacheableList(String name,
                                   Class<E> elementType,
                                   Expiration expiration,
                                   ListAccessor<E> accessor) {
        this.name = Objects.requireNonNull(name);
        this.elementType = Objects.requireNonNull(elementType);
        this.expiration = Objects.requireNonNull(expiration);
        this.accessor = Objects.requireNonNull(accessor);
        this.cachedAccessor = new NonFaultTolerantAccessor<>();
        this.synced = true;
        this.valueCached = false;
        this.descriptor = null;
    }

    public static <E> PersistedCacheableList<E> of(String name, Class<E> elementType) {
        return of(name, elementType, Expiration.none());
    }

    public static <E> PersistedCacheableList<E> of(
            String name, Class<E> elementType, Expiration expiration) {
        return new PersistedCacheableList<>(name, elementType, expiration, new NonFaultTolerantAccessor<>());
    }

    @Override
    public String name() { return name; }

    public Class<E> elementType(){ return elementType; }

    public Expiration expiration() {
        return expiration;
    }

    public Iterable<E> get() {
        int fetchedSize=0;
        if(synced){
            List<E> fetched = (List<E>)accessor.get();
            cachedAccessor.update(fetched);
            rDelta = 0;
            lDelta = 0;
            valueCached = true;
            return fetched.subList(0, fetched.size());
        }
        try {
            fetchedSize = accessor.size().intValue();
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (!valueCached){
            List<E> fetched = (List<E>)accessor.get();
            if(fetched == null ){
                fetched = new ArrayList<>();
            }

            fetched.addAll(cachedAccessor.list);
            cachedAccessor.update(fetched);
            valueCached = true;
        }

        List<Long> ret = (List<Long>)cachedAccessor.get();
        return (Iterable<E>) ret.subList(lDelta,  fetchedSize+rDelta);
    }

    public void add(@Nonnull E value){
        synced = false;
        rDelta++;
        cachedAccessor.add(value);
    }

    public void update(@Nonnull List<E> values) throws Exception {
        cachedAccessor.update(values);
        lDelta = accessor.size().intValue();
        rDelta = values.size();
        valueCached = true;
        synced = false;
    }

    public void addAll(@Nonnull List<E> values){
        synced = false;
        rDelta += values.size();
        cachedAccessor.addAll(values);

    }

    public E getIndex(int index) throws Exception {
        if (synced) return accessor.getIndex(lDelta + index);
        if (!valueCached){
            Long remoteSize = accessor.size();
            if(index + lDelta < remoteSize){
                return accessor.getIndex(index + lDelta);
            }
            return cachedAccessor.list.get(index + lDelta - remoteSize.intValue());
        }
        return cachedAccessor.getIndex(lDelta + index);
    }

    public E pollFirst() throws Exception {
        synced = false;
        lDelta++;
        if(!valueCached){
            Long remoteSize = accessor.size();
            if(remoteSize > lDelta){
                E poll = accessor.getIndex(lDelta);
                return poll;
            }
            return cachedAccessor.list.get(lDelta - remoteSize.intValue());
        }
        return cachedAccessor.list.get(lDelta);
    }

    public E pollLast() throws Exception {
        synced = false;
        rDelta--;
        if(!valueCached){
            if(cachedAccessor.size() + rDelta < 0)
                return accessor.getIndex(accessor.size().intValue() + (rDelta + cachedAccessor.size().intValue()));
        }

        return cachedAccessor.getIndex(cachedAccessor.size().intValue() + rDelta);
    }

    public Long size() throws Exception {
        if(synced) return accessor.size();
        return accessor.size() + rDelta - lDelta;
    }

    @Override
    public String toString() {
        return String.format(
                "PersistedList{name=%s, elementType=%s, expiration=%s}",
                name, elementType.getName(), expiration);
    }

    @ForRuntime
    void setAccessor(ListAccessor<E> newAccessor) {
        this.accessor = Objects.requireNonNull(newAccessor);
    }

    public void setDescriptor(StateDescriptor descriptor){ this.descriptor = descriptor; }

    @Override
    public void markDirty() {
        synced = false;
    }

    @Override
    public boolean isDirty() {
        return !synced;
    }

    @Override
    public void sync() throws Exception {
        if(synced) return;
        synced = true;
        int size = accessor.size().intValue();
        accessor.trim(lDelta, size - 1);
        if(rDelta < 0){
            size = accessor.size().intValue();
            accessor.trim(0, size  - 1 - rDelta);
        }
        else {
            if (!valueCached) {
                accessor.addAll(cachedAccessor.list.subList(0, rDelta));
            }
            else {
                accessor.addAll(cachedAccessor.list.subList(cachedAccessor.list.size() - rDelta, cachedAccessor.list.size()));
            }
        }
        valueCached = false;
        cachedAccessor.clear();
        lDelta = rDelta = 0;
    }

    @Override
    public void setInactive() {
        throw new NotImplementedException();
    }

    @Override
    public boolean ifActive() {
        throw new NotImplementedException();
    }

    @Override
    public void flush() {
        try {
            sync();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public StateDescriptor getDescriptor() {
        return descriptor;
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

        public void update(@Nonnull Iterable<E> values) {
            list.clear();
            values.forEach(list::add);
        }

        @Override
        public void addAll(@Nonnull List<E> values) {
            list.addAll(values);
        }

        public void addAll(@Nonnull Iterable<E> values) { values.forEach(list::add); }

        @Override
        public E getIndex(int index) {
            return list.get(index);
        }

        @Override
        public E pollFirst()  {
            return list.remove(0);
        }

        @Override
        public E pollLast()  {
            return list.remove(list.size() - 1);
        }

        @Override
        public void trim(int left, int right) throws Exception {
            list = list.subList(left, right + 1);
        }

        @Override
        public Long size()  {
            return (long)list.size();
        }

        public void clear(){
            list.clear();
        }
    }
}


//public final class PersistedCacheableList<E> implements CacheableState{
//    private final String name;
//    private final Class<E> elementType;
//    private final Expiration expiration;
//    private boolean synced;
//    private ListAccessor<E> accessor;
//    protected NonFaultTolerantAccessor<E> cachedAccessor;
//    private List<E> cachedAdded;
//    private int startIndex;
//    private int endIndex;
//
//    private PersistedCacheableList(String name,
//                                   Class<E> elementType,
//                                   Expiration expiration,
//                                   ListAccessor<E> accessor) {
//        this.name = Objects.requireNonNull(name);
//        this.elementType = Objects.requireNonNull(elementType);
//        this.expiration = Objects.requireNonNull(expiration);
//        this.accessor = Objects.requireNonNull(accessor);
//        this.cachedAccessor = new NonFaultTolerantAccessor<>();
//        this.cachedAdded = new ArrayList<>();
//    }
//
//    public static <E> PersistedCacheableList<E> of(String name, Class<E> elementType) {
//        return of(name, elementType, Expiration.none());
//    }
//
//    public static <E> PersistedCacheableList<E> of(
//            String name, Class<E> elementType, Expiration expiration) {
//        return new PersistedCacheableList<>(name, elementType, expiration, new NonFaultTolerantAccessor<>());
//    }
//
//    public String name() { return name; }
//
//    public Class<E> elementType(){ return elementType; }
//
//    public Expiration expiration() {
//        return expiration;
//    }
//
//    public Iterable<E> get(){
//        if(synced){
//            List<E> fetched = (List<E>)accessor.get();
//            cachedAccessor.update(fetched);
//            return fetched.subList(startIndex, fetched.size()-endIndex);
//        }
//        else if (cachedAccessor.size() == 0){
//            List<E> fetched = (List<E>)accessor.get();
//            int fetchedSize = (fetched == null? 0: fetched.size());
//            if(fetched != null && (fetchedSize > startIndex)){
//                cachedAccessor.update(fetched.subList(startIndex, fetched.size()));
//            }
//            cachedAccessor.addAll(cachedAdded.subList((fetchedSize > startIndex? 0:(startIndex -fetchedSize)) , cachedAdded.size()-endIndex));
//            startIndex = endIndex = 0;
//            cachedAdded.clear();
//        }
//
//        return cachedAccessor.get();
//    }
//
//    public void add(@Nonnull E value){
//        synced = false;
//        if(cachedAccessor.size()==0){
//            cachedAdded.add(value);
//            return;
//        }
//        cachedAccessor.add(value);
//    }
//
//    public void update(@Nonnull List<E> values){
//        cachedAccessor.update(values);
//        startIndex = endIndex = 0;
//        cachedAdded.clear();
//        synced = false;
//    }
//
//    public void addAll(@Nonnull List<E> values){
//        synced = false;
//        if(cachedAccessor.size() == 0){
//            cachedAdded.addAll(values);
//        }
//        else{
//            cachedAccessor.addAll(values);
//        }
//
//    }
//
//    public E getIndex(int index) throws Exception {
//        if (synced) return accessor.getIndex(startIndex + index);
//        if(cachedAccessor.size()==0){
//            Long remoteSize = accessor.size();
//            if(index + startIndex < remoteSize){
//                return accessor.getIndex(index + startIndex);
//            }
//            return cachedAdded.get(index + startIndex - remoteSize.intValue());
//        }
//        return cachedAccessor.getIndex(index);
//    }
//
//    public E pollFirst() throws Exception {
////        if(synced || (cachedAccessor.size()==0 && accessor.size()!=0)) {
////            return accessor.getIndex(0);
////        }
//        synced = false;
//        if(cachedAccessor.size() == 0){
//            Long remoteSize = accessor.size();
//            if(remoteSize > startIndex){
//                E poll = accessor.getIndex(startIndex);
//                startIndex++;
//                return poll;
//            }
//            return cachedAdded.get(startIndex ++ - remoteSize.intValue());
//        }
//        return cachedAccessor.pollFirst();
//    }
//
//    public E pollLast() throws Exception {
//        if(synced || (cachedAccessor.size()==0 && cachedAdded.size()==0)) accessor.pollLast();
//        synced = false;
//        if(cachedAccessor.size() ==0){
//            return cachedAdded.remove(cachedAdded.size()-1);
//        }
//        return cachedAccessor.pollLast();
//    }
//
//    public Long size() throws Exception {
//        if(synced) return accessor.size();
//        if(cachedAccessor.size()==0){
//            return accessor.size() + cachedAdded.size() - startIndex - endIndex;
//        }
//        return cachedAccessor.size();
//    }
//
//    @Override
//    public String toString() {
//        return String.format(
//                "PersistedList{name=%s, elementType=%s, expiration=%s}",
//                name, elementType.getName(), expiration);
//    }
//
//    @ForRuntime
//    void setAccessor(ListAccessor<E> newAccessor) {
//        //System.out.println("PersistedList setAccessor " + newAccessor);
//        this.accessor = Objects.requireNonNull(newAccessor);
//    }
//
//    @Override
//    public void markDirty() {
//        synced = false;
//    }
//
//    @Override
//    public boolean isDirty() {
//        return !synced;
//    }
//
//    @Override
//    public void sync() throws Exception {
//        if(synced) return;
//        synced = true;
//        if(cachedAccessor.size() == 0){
//            System.out.println("Syncing cacheable list " + this.name + " from cachedAdded size: " + cachedAdded.size());
//            int index = 0;
//            while(startIndex>index){
//                accessor.pollFirst();
//                index++;
//            }
//            if(endIndex > 0){
//                index = 0;
//                while(startIndex>index) {
//                    accessor.pollLast();
//                    index++;
//                }
//            }
//            else if (cachedAdded.size()>0){
//                accessor.addAll(cachedAdded);
//            }
//            cachedAdded.clear();
//            startIndex = endIndex = 0;
//            return;
//        }
//        System.out.println("Syncing cacheable list " + this.name + " from cachedAccessor size: " + cachedAccessor.size());
//        accessor.update((List<E>)cachedAccessor.get());
//        cachedAccessor.clear();
//    }
//
//    private static final class NonFaultTolerantAccessor<E> implements ListAccessor<E> {
//        private List<E> list = new ArrayList<>();
//
//        @Override
//        public Iterable<E> get() {
//            return list;
//        }
//
//        @Override
//        public void add(@Nonnull E value) {
//            list.add(value);
//        }
//
//        @Override
//        public void update(@Nonnull List<E> values) {
//            list = values;
//        }
//
//        public void update(@Nonnull Iterable<E> values) {
//            list.clear();
//            values.forEach(list::add);
//        }
//
//        @Override
//        public void addAll(@Nonnull List<E> values) {
//            list.addAll(values);
//        }
//
//        public void addAll(@Nonnull Iterable<E> values) { values.forEach(list::add); }
//
//        @Override
//        public E getIndex(int index) {
//            return list.get(index);
//        }
//
//        @Override
//        public E pollFirst()  {
//            return list.remove(0);
//        }
//
//        @Override
//        public E pollLast()  {
//            return list.remove(list.size() - 1);
//        }
//
//        @Override
//        public Long size()  {
//            return (long)list.size();
//        }
//
//        public void clear(){
//            list.clear();
//        }
//    }
//}









//public final class PersistedCacheableList<E> implements CacheableState{
//    private final String name;
//    private final Class<E> elementType;
//    private final Expiration expiration;
//    private boolean synced;
//    private ListAccessor<E> accessor;
//    protected NonFaultTolerantAccessor<E> cachedAccessor;
//    private List<E> cachedAdded;
//    private int startIndex;
//    private int endIndex;
//
//    private PersistedCacheableList(String name,
//                                   Class<E> elementType,
//                                   Expiration expiration,
//                                   ListAccessor<E> accessor) {
//        this.name = Objects.requireNonNull(name);
//        this.elementType = Objects.requireNonNull(elementType);
//        this.expiration = Objects.requireNonNull(expiration);
//        this.accessor = Objects.requireNonNull(accessor);
//        this.cachedAccessor = new NonFaultTolerantAccessor<>();
//        this.cachedAdded = new ArrayList<>();
//    }
//
//    public static <E> PersistedCacheableList<E> of(String name, Class<E> elementType) {
//        return of(name, elementType, Expiration.none());
//    }
//
//    public static <E> PersistedCacheableList<E> of(
//            String name, Class<E> elementType, Expiration expiration) {
//        return new PersistedCacheableList<>(name, elementType, expiration, new NonFaultTolerantAccessor<>());
//    }
//
//    public String name() { return name; }
//
//    public Class<E> elementType(){ return elementType; }
//
//    public Expiration expiration() {
//        return expiration;
//    }
//
//    public Iterable<E> get(){
//        if(synced){
//            List<E> fetched = (List<E>)accessor.get();
//            cachedAccessor.update(fetched);
//            return fetched.subList(startIndex, fetched.size()-endIndex);
//        }
//        else if (cachedAccessor.size() == 0){
//            List<E> fetched = (List<E>)accessor.get();
//            if(fetched != null){
//                cachedAccessor.update(fetched);
//            }
//
//            cachedAccessor.addAll(cachedAdded.subList(startIndex, cachedAdded.size()-endIndex));
//            cachedAdded.clear();
//        }
//
//        return cachedAccessor.get();
//    }
//
//    public void add(@Nonnull E value){
//        synced = false;
//        if(cachedAccessor.size()==0){
//            cachedAdded.add(value);
//            return;
//        }
//        cachedAccessor.add(value);
//    }
//
//    public void update(@Nonnull List<E> values){
//        cachedAccessor.update(values);
//        startIndex = endIndex = 0;
//        cachedAdded.clear();
//        synced = false;
//    }
//
//    public void addAll(@Nonnull List<E> values){
//        synced = false;
//        if(cachedAccessor.size() == 0){
//            cachedAdded.addAll(values);
//        }
//        else{
//            cachedAccessor.addAll(values);
//        }
//
//    }
//
//    public E getIndex(int index) throws Exception {
//        if (synced) return accessor.getIndex(startIndex + index);
//        if(cachedAccessor.size()==0){
//            Long remoteSize = accessor.size();
//            if(index + startIndex < remoteSize){
//                return accessor.getIndex(index + startIndex);
//            }
//            return cachedAdded.get(index + startIndex - remoteSize.intValue());
//        }
//        return cachedAccessor.getIndex(index);
//    }
//
//    public E pollFirst() throws Exception {
//        if(synced || (cachedAccessor.size()==0 && accessor.size()!=0)) return accessor.pollFirst();
//        synced = false;
//        if(cachedAccessor.size() == 0){
//            return cachedAdded.remove(0);
//        }
//        return cachedAccessor.pollFirst();
//    }
//
//    public E pollLast() throws Exception {
//        if(synced || (cachedAccessor.size()==0 && cachedAdded.size()==0)) accessor.pollLast();
//        synced = false;
//        if(cachedAccessor.size() ==0){
//            return cachedAdded.remove(cachedAdded.size()-1);
//        }
//        return cachedAccessor.pollLast();
//    }
//
//    public Long size() throws Exception {
//        if(synced) return accessor.size();
//        if(cachedAccessor.size()==0){
//            return accessor.size() + cachedAdded.size();
//        }
//        return cachedAccessor.size();
//    }
//
//    @Override
//    public String toString() {
//        return String.format(
//                "PersistedList{name=%s, elementType=%s, expiration=%s}",
//                name, elementType.getName(), expiration);
//    }
//
//    @ForRuntime
//    void setAccessor(ListAccessor<E> newAccessor) {
//        //System.out.println("PersistedList setAccessor " + newAccessor);
//        this.accessor = Objects.requireNonNull(newAccessor);
//    }
//
//    @Override
//    public void markDirty() {
//        synced = false;
//    }
//
//    @Override
//    public boolean isDirty() {
//        return !synced;
//    }
//
//    @Override
//    public void sync() throws Exception {
//        if(synced) return;
//        synced = true;
//        if(cachedAccessor.size() == 0){
//            int index = 0;
//            while(startIndex>index){
//                accessor.pollFirst();
//                index++;
//            }
//            if(endIndex > 0){
//                index = 0;
//                while(startIndex>index) {
//                    accessor.pollLast();
//                    index++;
//                }
//            }
//            else if (cachedAdded.size()>0){
//                accessor.addAll(cachedAdded);
//            }
//            cachedAdded.clear();
//            startIndex = endIndex = 0;
//            return;
//        }
//        accessor.update((List<E>)cachedAccessor.get());
//        cachedAccessor.clear();
//    }
//
//    private static final class NonFaultTolerantAccessor<E> implements ListAccessor<E> {
//        private List<E> list = new ArrayList<>();
//
//        @Override
//        public Iterable<E> get() {
//            return list;
//        }
//
//        @Override
//        public void add(@Nonnull E value) {
//            list.add(value);
//        }
//
//        @Override
//        public void update(@Nonnull List<E> values) {
//            list = values;
//        }
//
//        public void update(@Nonnull Iterable<E> values) {
//            list.clear();
//            values.forEach(list::add);
//        }
//
//        @Override
//        public void addAll(@Nonnull List<E> values) {
//            list.addAll(values);
//        }
//
//        public void addAll(@Nonnull Iterable<E> values) { values.forEach(list::add); }
//
//        @Override
//        public E getIndex(int index) {
//            return list.get(index);
//        }
//
//        @Override
//        public E pollFirst()  {
//            return list.remove(0);
//        }
//
//        @Override
//        public E pollLast()  {
//            return list.remove(list.size() - 1);
//        }
//
//        @Override
//        public Long size()  {
//            return (long)list.size();
//        }
//
//        public void clear(){
//            list.clear();
//        }
//    }
//}
