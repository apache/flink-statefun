package org.apache.flink.statefun.flink.core.message;

import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;

import static org.apache.flink.statefun.flink.core.StatefulFunctionsConfig.STATFUN_SCHEDULING;

public class FunctionRegistration extends InternalTypedSourceObject {
    public FunctionType functionType;
    public StatefulFunction statefulFunction;
    public String schedulingStrategyTag;

    public FunctionRegistration(FunctionType ft, StatefulFunction function){
        super();
        functionType = ft;
        statefulFunction = function;
        schedulingStrategyTag = STATFUN_SCHEDULING.defaultValue();
    }


    public FunctionRegistration(FunctionType ft, StatefulFunction function, String tag){
        super();
        functionType = ft;
        statefulFunction = function;
        schedulingStrategyTag = tag;
    }
}

