package org.apache.flink.statefun.flink.core.functions;

import java.io.Serializable;

public class SyncMessage implements Serializable {
    public enum Type{
        SYNC_ONE,
        SYNC_ALL
    }

    Type type;
    Boolean critical;
    Boolean autoblocking;

    public SyncMessage(Type injectedType, Boolean followedByCritical, Boolean stateReqAsBlocking){
        type = injectedType;
        critical = followedByCritical;
        autoblocking = stateReqAsBlocking;
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

    @Override
    public String toString(){
        return String.format("SyncMessage <Type: %s, Critical: %s>", type.toString(), critical.toString());
    }
}
