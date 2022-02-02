package org.apache.flink.statefun.flink.core.functions;

import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.InternalAddress;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.*;
import java.util.stream.Collectors;

public class RouteTracker {
    private final HashMap<InternalAddress, HashMap<InternalAddress, Boolean>> routes = new HashMap<>();
    private final HashMap<InternalAddress, HashSet<InternalAddress>> lessorToLessees = new HashMap<>();
    private final HashMap<InternalAddress, InternalAddress> lesseeToLessor = new HashMap<>();

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

    public void addLessee(Address lessor, Address lessee){
        lessorToLessees.putIfAbsent(lessor.toInternalAddress(), new HashSet<>());
        lessorToLessees.get(lessor.toInternalAddress()).add(lessee.toInternalAddress());
    }

    public List<Address> getLessees (Address lessor){
        if(lessorToLessees.containsKey(lessor.toInternalAddress())){
            return lessorToLessees.get(lessor.toInternalAddress()).stream().map(InternalAddress::toAddress).collect(Collectors.toList());
        }
        return null;
    }

    public boolean removeLessee(Address lessor, Address lessee){
        if(lessorToLessees.containsKey(lessor.toInternalAddress())){
            return lessorToLessees.get(lessor.toInternalAddress()).remove(lessee.toInternalAddress());
        }
        return false;
    }

    public boolean addLessor(Address lessee, Address lessor){
        InternalAddress previous = lesseeToLessor.put(lessee.toInternalAddress(), lessor.toInternalAddress());
        if(previous != null && !previous.equals(lessor.toInternalAddress())){
            throw new FlinkRuntimeException(String.format("Registering a different lessor %s to lessee %s, previous %s", lessor, lessee, previous));
        }
        return previous != null;
    }

    public Address getLessor(Address lessee){
        if(lesseeToLessor.containsKey(lessee.toInternalAddress())){
            return lesseeToLessor.get(lessee.toInternalAddress()).toAddress();
        }
        return null;
    }

    public boolean removeLessor(Address lessee){
        return lesseeToLessor.remove(lessee.toInternalAddress()) != null;
    }

    public String getLessorToLesseesMap(){
        return String.format("< %s >", lessorToLessees.entrySet().stream()
                .map(kv->kv.getKey().toAddress() + " -> " + Arrays.toString(kv.getValue().stream().map(InternalAddress::toAddress).toArray())).collect(Collectors.joining("|||")));
    }

    public String getLesseeToLessorMap(){
        return String.format("< %s >", lesseeToLessor.entrySet().stream()
                .map(kv->kv.getKey().toAddress() + " -> " + kv.getValue().toAddress()).collect(Collectors.joining("|||")));
    }
}
