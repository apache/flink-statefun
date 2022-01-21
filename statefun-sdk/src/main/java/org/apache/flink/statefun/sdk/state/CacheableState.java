package org.apache.flink.statefun.sdk.state;

public abstract class CacheableState extends ManagedState{

    abstract void markDirty();

    abstract boolean isDirty();

    abstract void sync() throws Exception;

    @Override
    public Boolean ifNonFaultTolerance() {
        return true;
    }

//    @Override
//    public void setInactive(){ }
}
