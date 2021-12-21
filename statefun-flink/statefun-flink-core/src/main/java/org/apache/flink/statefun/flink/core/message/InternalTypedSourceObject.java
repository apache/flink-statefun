package org.apache.flink.statefun.flink.core.message;

import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.statefun.sdk.FunctionType;

import java.io.Serializable;
import java.util.Objects;

public class InternalTypedSourceObject implements Comparable, Serializable {

    private FunctionType internalType;

    private int subIndex;

    private Long priority = 0L;

    private long laxity = 0L;

    private int translatedOperatorIndex;

    private final Long nano;

    public FunctionType getInternalType() {
        return internalType;
    }

    public void setInternalType(FunctionType internal) {
        internalType = internal;
    }

    public int getSubIndex() {
        return subIndex;
    }

    public void setSubIndex(int index) {
        subIndex = index;
    }

    public Long getPriority() {
        return priority;
    }

    public void setPriority(Long priority) {
        this.priority = priority;
    }

    public Long getLaxity() {
        return laxity;
    }

    public Long getNano() { return nano; }

    public void setLaxity(Long laxity) {
        this.laxity = laxity;
    }

    public void setTranslatedOperatorIndex(int maxParallelism, int parallelism, int taskIndex) {
        translatedOperatorIndex = KeyGroupRangeAssignment.computeKeyGroupForOperatorIndex(
                maxParallelism,
                parallelism,
                taskIndex);
    }

    public int getTranslatedOperatorIndex() {
        return translatedOperatorIndex;
    }

    public InternalTypedSourceObject(){
        nano = System.nanoTime();
    }

    @Override
    public int compareTo(Object o) {
        if(o == null){
            return 1;
        }
        int ret = this.priority.compareTo(Objects.requireNonNull((InternalTypedSourceObject)o).priority);
        if(ret!=0) return ret;
        ret = this.nano.compareTo(Objects.requireNonNull((InternalTypedSourceObject)o).nano);
        if(ret!=0) return ret;
        return hashCode() - o.hashCode();
    }
}
