package org.apache.flink.statefun.sdk;

public abstract class BaseStatefulFunction implements StatefulFunction {
    public abstract boolean statefulSubFunction(Address addressDetails);

    public abstract String getCurrentFunctionId();

    public abstract String getStrategyTag(Address address);

    public abstract Integer getNumUpstreams(Address address);
}
