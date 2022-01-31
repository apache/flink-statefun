package org.apache.flink.statefun.flink.core.functions.utils;

import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.sdk.Address;

public class RuntimeUtils {
    public static String messageToID (Message message){
        return String.format("source <%s> target <%s> id <%s>",
                message.source().toString(),
                message.target().toString(),
                message.getMessageId().toString());
    }

    public static String sourceToPrefix (Address source){
        return String.format("source <%s>", source.toString());
    }
}
