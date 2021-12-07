package org.apache.flink.statefun.sdk.state;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;

public abstract class ManagedState {
    public abstract Boolean ifNonFaultTolerance();
    public abstract void setInactive();
    public abstract void flush();
    public boolean ifPartitioned(){
        return false;
    }
    public abstract StateDescriptor getDescriptor();
}
