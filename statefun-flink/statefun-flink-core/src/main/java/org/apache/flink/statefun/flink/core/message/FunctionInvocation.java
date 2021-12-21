package org.apache.flink.statefun.flink.core.message;

import org.apache.flink.statefun.sdk.FunctionType;

public class FunctionInvocation extends InternalTypedSourceObject {
    public FunctionType functionType;
    public Object messageWrapper;

    public FunctionInvocation(FunctionType ft, Object message){
        super();
        functionType = ft;
        messageWrapper = message;
    }
}
