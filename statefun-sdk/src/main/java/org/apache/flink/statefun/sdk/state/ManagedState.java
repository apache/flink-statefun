package org.apache.flink.statefun.sdk.state;

public abstract class ManagedState {
    public abstract Boolean ifNonFaultTolerance();
    public abstract void setInactive();
    public abstract void flush();
    public boolean ifPartitioned(){
        return false;
    }
}
