package org.apache.flink.statefun.flink.core.functions.utils;

import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.PriorityBlockingQueue;

public class PriorityBasedWorkQueue<T extends Comparable> implements WorkQueue<T> {

    PriorityBlockingQueue<T> workQueue;

    public PriorityBasedWorkQueue(){
        workQueue = new PriorityBlockingQueue<>();
    }

    public PriorityBasedWorkQueue(PriorityBlockingQueue<T> queue){
        workQueue = new PriorityBlockingQueue<>(queue);
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
        PriorityBasedWorkQueue<T> duplicate = new PriorityBasedWorkQueue<>(workQueue);
        SortedSet<T> ret = new TreeSet<>();
        while(duplicate.size()>0){
            T polled = duplicate.poll();
            if(polled.compareTo(obj)<=0){
                continue;
            }
            ret.add(polled);
        }
        return ret;
    }

    @Override
    public WorkQueue<T> copy() {
        return new PriorityBasedWorkQueue<>(workQueue);
    }

    @Override
    public Iterable<T> toIterable() {
        return workQueue;
    }
}
