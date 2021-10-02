package org.apache.flink.statefun.sdk.utils;

import org.apache.flink.statefun.sdk.FunctionType;

public class DataflowUtils {
    public static FunctionType toJobFunctionType(String namespace, short functionId, short jobId, short partitionId)
    {
        long type = 0L;
        type  = type << 16 | functionId;
        type = type << 16 | jobId;
        type = type << 16 | partitionId;
        return new FunctionType(namespace, String.valueOf(type));
    }

    public static short getFunctionId (FunctionType type){
        long rawId = Long.valueOf(type.name());
        return (short) ((rawId >> 32) & ((1L << 16)-1));
    }

    public static short getJobId (FunctionType type){
        long rawId = Long.valueOf(type.name());
        return (short) (rawId >> 16 & ((1L << 16)-1));
    }

    public static short getPartitionId (FunctionType type){
        long rawId = Long.valueOf(type.name());
        return (short) (rawId & ((1L << 16)-1));
    }

    public static String typeToFunctionTypeString(FunctionType type){
        return String.format("Namespace: " + type.namespace()
                + " FunctionId: " + getFunctionId(type)
                + " JobId: " + getJobId(type)
                + " PartitionId: " + getPartitionId(type)
        );
    }
}
