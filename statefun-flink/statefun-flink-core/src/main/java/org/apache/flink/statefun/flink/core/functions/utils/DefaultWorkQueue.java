package org.apache.flink.statefun.flink.core.functions.utils;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import java.util.ArrayDeque;
import java.util.Set;

public class DefaultWorkQueue<T> implements WorkQueue<T> {
    ArrayDeque<T> workQueue;

    public DefaultWorkQueue(){
        workQueue = new ArrayDeque<>();
    }

    public DefaultWorkQueue(ArrayDeque<T> workQueue){
        this.workQueue = new ArrayDeque<>(workQueue);
    }

    @Override
    public T poll() {
        return workQueue.poll();
    }

    @Override
    public T peek() {
        return workQueue.peek();
    }

    @Override
    public boolean contains(T obj) {
        return workQueue.contains(obj);
    }

    @Override
    public boolean add(T obj) {
        return workQueue.add(obj);
    }

    @Override
    public boolean remove(T obj) {
        return workQueue.remove(obj);
    }

    @Override
    public int size() {
        return workQueue.size();
    }

    @Override
    public Set<T> tailSet(T obj) {
        throw new NotImplementedException();
    }

    @Override
    public WorkQueue<T> copy() {
        return new DefaultWorkQueue<>(workQueue);
    }

    @Override
    public Iterable<T> toIterable() {
        return workQueue;
    }
}
