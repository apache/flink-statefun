package org.apache.flink.statefun.flink.core.functions;

import javafx.util.Pair;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.InternalAddress;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RouteTracker {
    private final HashMap<InternalAddress, HashMap<InternalAddress, Boolean>> routes = new HashMap<>();

    // Add address mapping when scheduler forward a message
    // A message sent from initiator to a PA on behalf of VA
    public void activateRoute(Address initiator, Address pa){
        routes.putIfAbsent(initiator.toInternalAddress(), new HashMap<>());
        routes.get(initiator.toInternalAddress()).put(pa.toInternalAddress(), true);
    }


    // disable a route when there is no on-the-fly messages
    // The route stay active until a message is received from pa
    public boolean disableRoute(Address initiator, Address pa){
        if(routes.containsKey(initiator.toInternalAddress()) && routes.get(initiator.toInternalAddress()).containsKey(pa.toInternalAddress())){
            routes.get(initiator.toInternalAddress()).put(pa.toInternalAddress(),  false);
            return true;
        }
        return false;
    }


    // Erase a route when the route is inactive and there is no side-effect on lessee (e.g., function states)
    public boolean removeRoute(Address initiator, Address pa){
        if(routes.containsKey(initiator.toInternalAddress()) && routes.get(initiator.toInternalAddress()).containsKey(pa.toInternalAddress())){
            return routes.get(initiator.toInternalAddress()).remove(pa.toInternalAddress()) != null;
        }
        return false;
    }


    // get all active PAs associated with a VA created by initiator
    public List<Address> getAllActiveRoutes (Address initiator){
        if(routes.containsKey(initiator.toInternalAddress())){
            return  routes.get(initiator.toInternalAddress()).entrySet().stream().filter(Map.Entry::getValue).map(kv->kv.getKey().toAddress()).collect(Collectors.toList());
        }
        return new ArrayList<>();
    }


    // get all PAs associated with a VA created by initiator
    public Address[] getAllRoutes(Address initiator){
        if(routes.containsKey(initiator.toInternalAddress())){
            return (Address[]) routes.get(initiator.toInternalAddress()).keySet().stream().map(InternalAddress::toAddress).toArray();
        }
        return new Address[0];
    }
}
