package org.apache.flink.statefun.flink.core.functions;

import org.apache.flink.statefun.flink.core.message.InternalTypedSourceObject;
import org.apache.flink.statefun.sdk.FunctionType;

public class FunctionInvocation extends InternalTypedSourceObject {
    public FunctionType functionType;
    public Object messageWrapper;
    public Boolean ifCritical = null;

    public FunctionInvocation(FunctionType ft, Object message){
        super();
        functionType = ft;
        messageWrapper = message;
    }

    public FunctionInvocation(FunctionType ft, Object message, Boolean critical){
        super();
        functionType = ft;
        messageWrapper = message;
        ifCritical = critical;
    }
}
