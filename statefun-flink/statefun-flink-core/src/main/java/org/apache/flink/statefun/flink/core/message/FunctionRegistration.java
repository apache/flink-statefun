package org.apache.flink.statefun.flink.core.message;

import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;

public class FunctionRegistration extends InternalTypedSourceObject {
    public FunctionType functionType;
    public StatefulFunction statefulFunction;

    public FunctionRegistration(FunctionType ft, StatefulFunction function){
        super();
        functionType = ft;
        statefulFunction = function;
    }
}

