package org.apache.flink.statefun.flink.core.functions.utils;

public abstract class MinLaxityWorkQueue<T extends Comparable> implements WorkQueue<T> {

    public abstract boolean tryInsertWithLaxityCheck(T item);

    public abstract boolean laxityCheck(T item);

    public abstract boolean tryInsertWithLaxityCheck(T item, long baseTime);

    public abstract boolean laxityCheck(T item, long baseTime);

    public abstract String dumpLaxityMap();
}
