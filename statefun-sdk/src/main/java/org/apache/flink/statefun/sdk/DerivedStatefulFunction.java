package org.apache.flink.statefun.sdk;

public abstract class DerivedStatefulFunction implements StatefulFunction{
    protected BaseStatefulFunction baseFunction;

    public void setBaseFunction(BaseStatefulFunction function){
        baseFunction = function;
    }

    public abstract Boolean ifStateful();

    public void flushState(){}
}
