package org.apache.flink.statefun.sdk.state;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.ArrayList;

public abstract class ManagedState {
    public enum Mode{
        EXCLUSIVE,
        SHARED
    }
    public abstract Boolean ifNonFaultTolerance();
    public abstract void setInactive();
    public abstract boolean ifActive();
    public abstract void flush();
    public abstract String name();
    public boolean ifPartitioned(){
        return false;
    }
    public abstract StateDescriptor getDescriptor();

    protected Address lessor;
    protected ArrayList<Address> accessors = new ArrayList<>();
    private Mode mode;

    public Address getLessor(){
        return lessor;
    }

    public ArrayList<Address> getAccessors(){
        return accessors;
    }

    public void updateAccessors(Address accessor){
        if(mode == Mode.EXCLUSIVE){
            accessors.clear();
            accessors.add(accessor);
        }
        else{
            accessors.add(accessor);
        }
    }

    public void revokeSharedAccesses(){
        if( mode == Mode.EXCLUSIVE){
            throw new FlinkRuntimeException("Cannot revoke shared accesses under Exclude mode for state " + name());
        }
        else{
            accessors.clear();
            accessors.add(getLessor());
        }
    }

    public Mode getMode(){
        return mode;
    }

    protected void setMode(Mode mode){
        this.mode = mode;
    }

}
