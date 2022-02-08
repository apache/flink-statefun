package org.apache.flink.statefun.flink.core.functions;

import javafx.util.Pair;
import org.apache.flink.statefun.sdk.Address;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SyncReplyState implements Serializable {
    private HashMap<Pair<String, Address>, byte[]> stateMap;
    private List<Address> targetList;

    public SyncReplyState(HashMap<Pair<String, Address>, byte[]> map, List<Address> list){
        stateMap = map;
        targetList = list;
    }

    public HashMap<Pair<String, Address>, byte[]> getStateMap(){
        return stateMap;
    }

    public List<Address> getTargetList(){
        return targetList;
    }
}
