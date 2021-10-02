package org.apache.flink.statefun.flink.core.functions.scheduler;

import org.apache.flink.statefun.sdk.Address;

public class MetaState {
    public Boolean redirectable;
    public Boolean flushing;
    public Address sequencer;
    public Long fenceId;

    public MetaState(Boolean redirectable){
        this.redirectable = redirectable;
        this.flushing = false;
    }

    public MetaState(Boolean redirectable, Boolean flushing){
        this.redirectable = redirectable;
        this.flushing = flushing;
    }

    public MetaState(Boolean redirectable, Boolean flushing, Address sequencer, Long fenceId){
        this.redirectable = redirectable;
        this.flushing = flushing;
        this.sequencer = sequencer;
        this.fenceId = fenceId;
    }
}
