package org.apache.flink.statefun.sdk;

// TODO: Add function progress here
public abstract class TrackableStatefulFunction extends BaseStatefulFunction {
    abstract Long getProgress();
}
