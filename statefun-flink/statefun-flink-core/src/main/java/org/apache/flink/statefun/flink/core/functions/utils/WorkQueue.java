package org.apache.flink.statefun.flink.core.functions.utils;

import java.util.Set;

public interface WorkQueue<T>  {
    T poll();

    T peek();

    boolean contains(T obj);

    boolean add(T obj);

    boolean remove(T obj);

    int size();

    Set<T> tailSet(T obj);

    WorkQueue<T> copy();

    public Iterable<T> toIterable();
}
