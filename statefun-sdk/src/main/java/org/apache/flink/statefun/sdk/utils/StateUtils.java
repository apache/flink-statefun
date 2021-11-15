package org.apache.flink.statefun.sdk.utils;

import java.util.ArrayList;
import java.util.List;

public class StateUtils {
    public static List<String> getPartitionedStateNames(String statename, Integer numPartitions){
        ArrayList<String> ret = new ArrayList<>();
        for(int i = 0; i < numPartitions; i++){
            ret.add(statename + " p" + i);
        }
        return ret;
    }
}
