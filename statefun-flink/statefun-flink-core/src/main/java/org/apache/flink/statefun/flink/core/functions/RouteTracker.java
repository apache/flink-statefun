package org.apache.flink.statefun.flink.core.functions;

import javafx.util.Pair;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.InternalAddress;
import org.apache.flink.statefun.sdk.utils.DataflowUtils;
import org.apache.flink.util.FlinkRuntimeException;
import scala.Int;

import java.util.*;
import java.util.stream.Collectors;

public class RouteTracker {
    private final HashMap<InternalAddress, HashMap<InternalAddress, Boolean>> routes = new HashMap<>();
    private final HashMap<InternalAddress, HashSet<InternalAddress>> lessorToLessees = new HashMap<>();
    private final HashMap<InternalAddress, InternalAddress> lesseeToLessor = new HashMap<>();

    private final HashMap<InternalAddress, HashSet<InternalAddress>> targetToSourceRoutes = new HashMap<>();
    private final HashMap<InternalAddress, Pair<HashSet<InternalAddress>, HashSet<InternalAddress>>> targetToFlushedChannels = new HashMap<>();

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

    public List<Address> getAllActiveDownstreamRoutes (Address initiator){
        if(routes.containsKey(initiator.toInternalAddress())){
            return  routes.get(initiator.toInternalAddress()).entrySet().stream()
                    .filter(pair-> pair.getValue() && (DataflowUtils.getFunctionId(pair.getKey().toAddress().type().getInternalType()) > DataflowUtils.getFunctionId(initiator.type().getInternalType())))
                    .map(kv->kv.getKey().toAddress()).collect(Collectors.toList());
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

    public boolean ifLessorOf(Address lessor, Address lessee){
        if(!lesseeToLessor.containsKey(lessee.toInternalAddress())) return false;
        return lesseeToLessor.get(lessee.toInternalAddress()).equals(lessor.toInternalAddress());
    }

    public boolean ifLesseeOf(Address lessee, Address lessor){
        if(!lessorToLessees.containsKey(lessor.toInternalAddress())) return false;
        return lessorToLessees.get(lessor.toInternalAddress()).contains(lessee.toInternalAddress());
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

    public void mergeTemporaryRoutingEntries(Address source, List<Address> targets){
        for(Address target : targets){
            targetToSourceRoutes.putIfAbsent(target.toInternalAddress(), new HashSet<>());
            targetToSourceRoutes.get(target.toInternalAddress()).add(source.toInternalAddress());

        }
    }

    public Map<InternalAddress, List<Address>> getTemporaryTargetToSourcesRoutes(){
        return targetToSourceRoutes.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, kv->kv.getValue().stream().map(InternalAddress::toAddress).collect(Collectors.toList())));//.map(kv->new Map.Entry<>(kv.getKey().toAddress(), kv.getValue().stream().map(InternalAddress::toAddress).collect(Collectors.toList()))).coll;
    }

    public void clearTemporaryRoutingEntries(){
        targetToSourceRoutes.clear();
    }

    public void onFlushDependencyReceive(Address target, List<Address> dependencies){
        targetToFlushedChannels.putIfAbsent(target.toInternalAddress(), new Pair<>(new HashSet<>(), new HashSet<>()));
        targetToFlushedChannels.get(target.toInternalAddress()).getKey().addAll(dependencies.stream().map(Address::toInternalAddress).collect(Collectors.toList()));
    }

    public void onFlushReceive(Address target, Address flushReceived){
        targetToFlushedChannels.putIfAbsent(target.toInternalAddress(), new Pair<>(new HashSet<>(), new HashSet<>()));
        targetToFlushedChannels.get(target.toInternalAddress()).getValue().add(flushReceived.toInternalAddress());
    }

    public boolean ifUpstreamFlushed(Address target){
        Pair<HashSet<InternalAddress>, HashSet<InternalAddress>> flushes = targetToFlushedChannels.get(target.toInternalAddress());
        if(flushes== null) {
            System.out.println("Target address: " + target + " has no upstream flushes. tid: " + Thread.currentThread().getName());
            return true;
        }
        return flushes.getKey().equals(flushes.getValue());
    }

    public void clearFlushDependencyReceived(Address target){
        targetToFlushedChannels.remove(target.toInternalAddress());
    }

    public String getTargetToSourceRoutes() {
        return targetToSourceRoutes.entrySet().stream().map(kv-> kv.getKey() + " -> " + Arrays.toString(kv.getValue().toArray())).collect(Collectors.joining("|||"));
    }

    public String getTargetToFlushedChannels() {
        return targetToFlushedChannels.entrySet().stream()
                .map(kv-> kv.getKey() + " -> (" + Arrays.toString(kv.getValue().getKey().toArray()) + " == " + Arrays.toString(kv.getValue().getValue().toArray()) + ")")
                .collect(Collectors.joining(" ||| "));
    }
}


