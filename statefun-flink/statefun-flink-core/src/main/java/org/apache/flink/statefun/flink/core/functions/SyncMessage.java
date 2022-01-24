package org.apache.flink.statefun.flink.core.functions;

import org.apache.flink.statefun.sdk.Address;

import java.io.Serializable;

public class SyncMessage implements Serializable {
    public enum Type{
        SYNC_ONE,
        SYNC_ALL
    }

    Type type;
    Boolean critical;
    Boolean autoblocking;
    Address initializationSource = null;
    Integer numDependencies = null;

    public SyncMessage(Type injectedType, Boolean followedByCritical, Boolean stateReqAsBlocking){
        type = injectedType;
        critical = followedByCritical;
        autoblocking = stateReqAsBlocking;
    }

    public SyncMessage(Type injectedType, Boolean followedByCritical, Boolean stateReqAsBlocking, Address source){
        type = injectedType;
        critical = followedByCritical;
        autoblocking = stateReqAsBlocking;
        initializationSource = source;
    }

    public SyncMessage(Type injectedType, Boolean followedByCritical, Boolean stateReqAsBlocking, Address source, Integer dependencies){
        type = injectedType;
        critical = followedByCritical;
        autoblocking = stateReqAsBlocking;
        initializationSource = source;
        numDependencies = dependencies;
    }

    public boolean ifSyncAll(){
        return type.equals(Type.SYNC_ALL);
    }

    public boolean ifCritical(){
        return critical;
    }

    public boolean ifAutoBlocking(){
        return autoblocking;
    }

    public Address getInitializationSource() { return initializationSource; }

    public Integer getNumDependencies(){ return numDependencies; }

    @Override
    public String toString(){
        return String.format("SyncMessage <type %s, critical %s, autoblocking %s, initializationSource %s, numDependencies %s>",
                type.toString(), critical.toString(), autoblocking.toString(),
                (initializationSource == null?"null":initializationSource.toString()),
                (numDependencies == null?"null":numDependencies.toString()));
    }
}
