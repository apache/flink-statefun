package org.apache.flink.statefun.flink.core.functions.utils;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

public class SkipListBasedWorkQueue<T extends Comparable> implements WorkQueue<T> {

    ConcurrentSkipListSet<T> concurrentSkipListSet;

    public SkipListBasedWorkQueue(){
        this.concurrentSkipListSet = new ConcurrentSkipListSet<>();
    }

    public SkipListBasedWorkQueue(ConcurrentSkipListSet<T> concurrentSkipListSet){
        this.concurrentSkipListSet = new ConcurrentSkipListSet<>(concurrentSkipListSet);
    }

    @Override
    public T poll() {
        return concurrentSkipListSet.pollFirst();
    }

    @Override
    public T peek() {
        return concurrentSkipListSet.first();
    }

    @Override
    public boolean contains(T obj) {
        return concurrentSkipListSet.contains(obj);
    }

    @Override
    public boolean add(T obj) {
        return concurrentSkipListSet.add(obj);
    }

    @Override
    public boolean remove(T obj) {
        return concurrentSkipListSet.remove(obj);
    }

    @Override
    public int size() {
        return concurrentSkipListSet.size();
    }

    @Override
    public Set<T> tailSet(T obj) {
        return concurrentSkipListSet.tailSet(obj);
    }

    @Override
    public WorkQueue<T> copy() {
        return new SkipListBasedWorkQueue<T>(concurrentSkipListSet);
    }

    @Override
    public Iterable<T> toIterable() {
        return concurrentSkipListSet;
    }
}
