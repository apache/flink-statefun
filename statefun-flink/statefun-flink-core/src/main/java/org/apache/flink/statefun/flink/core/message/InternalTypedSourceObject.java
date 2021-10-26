package org.apache.flink.statefun.flink.core.message;

import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.statefun.sdk.FunctionType;

import java.io.Serializable;

public class InternalTypedSourceObject implements Serializable {

    private FunctionType internalType;

    private int subIndex;

    private long priority = 0L;

    private long laxity = 0L;

    private int translatedOperatorIndex;

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
}
