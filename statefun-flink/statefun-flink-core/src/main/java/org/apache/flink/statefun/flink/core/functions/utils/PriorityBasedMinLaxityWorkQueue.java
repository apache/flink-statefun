package org.apache.flink.statefun.flink.core.functions.utils;

import org.apache.flink.statefun.flink.core.functions.FunctionActivation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;


public class PriorityBasedMinLaxityWorkQueue<T extends LaxityComparableObject> extends MinLaxityWorkQueue<LaxityComparableObject> {

    TreeMap<LaxityComparableObject, Long> workQueue;
    TreeMap<LaxityComparableObject, Long> laxityMap;
    private static final Logger LOG = LoggerFactory.getLogger(FunctionActivation.class);

    public PriorityBasedMinLaxityWorkQueue(){
        this.workQueue = new TreeMap<>();
        this.laxityMap = new TreeMap<>();
    }

    public PriorityBasedMinLaxityWorkQueue(TreeMap<LaxityComparableObject, Long> workQueue,
                                           TreeMap<LaxityComparableObject, Long> laxityMap){
        this.workQueue = new TreeMap<>(workQueue);
        this.laxityMap = new TreeMap<>(laxityMap);
    }

    @Override
    public boolean tryInsertWithLaxityCheck(LaxityComparableObject obj) {
//        Long start = System.nanoTime();
        try {
            if(workQueue.size()==0) {
                Long currentMillis = System.currentTimeMillis();
                if(obj.getPriority().laxity < currentMillis ){
                    return false;
                }
                workQueue.put(obj, obj.getPriority().laxity);
                laxityMap.put(obj, obj.getPriority().laxity);
                return true;
            }
            else{
                Long currentMillis = System.currentTimeMillis();
                Long laxity = obj.getPriority().laxity;
                Long ec = obj.getPriority().priority - obj.getPriority().laxity;
                Long minLaxitySoFar = Long.MAX_VALUE;
                Long minLaxityHeadMap = Long.MAX_VALUE;
                boolean preCheck = false;
                boolean insert = false;
                // Check the floor key and find execution cost of all previous entry
                Map.Entry<LaxityComparableObject, Long> floorEntry = workQueue.floorEntry(obj);
                // original deadline - current laxity = all execution costs
                if(floorEntry!=null){
                    Long ecTotal = floorEntry.getKey().getPriority().priority - floorEntry.getValue();
                    if(currentMillis + ecTotal > obj.getPriority().laxity){
                        return false;
                    }
                    laxity -= ecTotal;
                }
                for(Map.Entry<LaxityComparableObject, Long> item :  workQueue.descendingMap().headMap(obj).entrySet()){
//                    if(item.getKey().compareTo(obj) > 0){
                        if(!preCheck){
                            // check before touch laxity stored
                            Map.Entry<LaxityComparableObject, Long> nextItem = laxityMap.ceilingEntry(obj);
                            if(nextItem!=null && (nextItem.getValue() - ec < currentMillis)){
                                return false;
                            }
                            preCheck = true;
                        }
                        Long updatedLaxity = item.getValue() - ec;
                        item.setValue(updatedLaxity);
                        if(minLaxitySoFar > updatedLaxity){
                            minLaxitySoFar = updatedLaxity;
                            laxityMap.put(item.getKey(), updatedLaxity);
                        }
                        if(updatedLaxity < minLaxityHeadMap){
                            minLaxityHeadMap = updatedLaxity;
                        }
//                    }
//                    else{
//                        // guarantee not equal
//                        laxity -= (item.getKey().getPriority().priority - item.getKey().getPriority().laxity);
//                        if(laxity < currentMillis) return false;
//                    }
                }
                if(minLaxityHeadMap > laxity) {
                    insert = true;
                    minLaxityHeadMap = laxity;
                }
                SortedMap<LaxityComparableObject, Long> headLaxityMap = laxityMap.headMap(obj);

                Iterator<Map.Entry<LaxityComparableObject, Long>> iter = headLaxityMap.entrySet().iterator();

                while(iter.hasNext()){
                    Map.Entry<LaxityComparableObject, Long> item = iter.next();
                    if(item.getValue() >= minLaxityHeadMap) {
                        // remove
                        iter.remove();
//                        if(!insert){
//                            insert = true;
//                        }
                    }
                }
                if(insert) laxityMap.put(obj, laxity);
                workQueue.put(obj, laxity);
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
//        finally {
//            System.out.println("queue time tryInsertWithLaxityCheck " + (System.nanoTime() - start) + " size " + workQueue.size());
//        }
        return false;
    }

    // Test only
    @Override
    public boolean tryInsertWithLaxityCheck(LaxityComparableObject obj, long baseTime) {
//        Long start = System.nanoTime();
        try {
            if(workQueue.size()==0) {
                Long currentMillis = baseTime;
//                currentMillis = baseTime;
                if(obj.getPriority().laxity < currentMillis ){
                    return false;
                }
                workQueue.put(obj, obj.getPriority().laxity);
                laxityMap.put(obj, obj.getPriority().laxity);
                return true;
            }
            else{
                Long currentMillis = 0L; //System.currentTimeMillis();
                Long laxity = obj.getPriority().laxity;
                Long ec = obj.getPriority().priority - obj.getPriority().laxity;
                Long minLaxitySoFar = Long.MAX_VALUE;
                Long minLaxityHeadMap = Long.MAX_VALUE;
                boolean preCheck = false;
                boolean insert = false;
                // Check the floor key and find execution cost of all previous entry
                Map.Entry<LaxityComparableObject, Long> floorEntry = workQueue.floorEntry(obj);
                if(floorEntry!=null){
                    // original deadline - current laxity = all execution costs
                    Long ecTotal = floorEntry.getKey().getPriority().priority - floorEntry.getValue();
                    if(currentMillis + ecTotal > obj.getPriority().laxity){
                        return false;
                    }
                    laxity -= ecTotal;
                }

                for(Map.Entry<LaxityComparableObject, Long> item :  workQueue.descendingMap().headMap(obj).entrySet()){
//                    if(item.getKey().compareTo(obj) > 0){
                    if(!preCheck){
                        // check before touch laxity stored
                        Map.Entry<LaxityComparableObject, Long> nextItem = laxityMap.ceilingEntry(obj);
                        if(nextItem!=null && (nextItem.getValue() - ec < currentMillis)){
                            return false;
                        }
                        preCheck = true;
                    }
                    Long updatedLaxity = item.getValue() - ec;
                    item.setValue(updatedLaxity);
                    if(minLaxitySoFar > updatedLaxity){
                        minLaxitySoFar = updatedLaxity;
                        laxityMap.put(item.getKey(), updatedLaxity);
                    }
                    if(updatedLaxity < minLaxityHeadMap){
                        minLaxityHeadMap = updatedLaxity;
                    }
//                    }
//                    else{
//                        // guarantee not equal
//                        laxity -= (item.getKey().getPriority().priority - item.getKey().getPriority().laxity);
//                        if(laxity < currentMillis) return false;
//                    }
                }
                if(minLaxityHeadMap > laxity) {
                    insert = true;
                    minLaxityHeadMap = laxity;
                }
                SortedMap<LaxityComparableObject, Long> headLaxityMap = laxityMap.headMap(obj);

                Iterator<Map.Entry<LaxityComparableObject, Long>> iter = headLaxityMap.entrySet().iterator();

                while(iter.hasNext()){
                    Map.Entry<LaxityComparableObject, Long> item = iter.next();
                    if(item.getValue() >= minLaxityHeadMap) {
                        // remove
                        iter.remove();
//                        if(!insert){
//                            insert = true;
//                        }
                    }
                }
                if(insert) laxityMap.put(obj, laxity);
                workQueue.put(obj, laxity);
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
//        finally {
//            System.out.println("queue time tryInsertWithLaxityCheck " + (System.nanoTime() - start) + " size " + workQueue.size());
//        }
        return false;
    }

    @Override
    public boolean laxityCheck(LaxityComparableObject obj) {
//        Long start = System.nanoTime();
        try{
            Long currentMillis = System.currentTimeMillis();
            if(workQueue.size()==0) {
                if(obj.getPriority().laxity < currentMillis ){
                    return false;
                }
                return true;
            }

//            Long ecTotal = workQueue.headMap(obj).entrySet().stream().mapToLong(kv-> {
//                try {
//                    return (kv.getKey().getPriority().priority - kv.getKey().getPriority().laxity);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//                return Long.MAX_VALUE;
//            }).sum();
            // Check the floor key and find execution cost of all previous entry
            Map.Entry<LaxityComparableObject, Long> floorEntry = workQueue.floorEntry(obj);
            Long ecTotal = 0L;
            if(floorEntry != null){
                // original deadline - current laxity = all execution costs
                ecTotal = floorEntry.getKey().getPriority().priority - floorEntry.getValue();
            }
//            LOG.debug("laxityCheck LaxityComparableObject 1. " + obj + " ec total " + ecTotal + " currentMillis " + currentMillis
//                    + " queue size " + workQueue.size() + " queue detail " + toString());
            if(ecTotal + currentMillis <= obj.getPriority().laxity){
                Map.Entry<LaxityComparableObject,Long> ceilingLaxityEntry = laxityMap.ceilingEntry(obj);
                if(ceilingLaxityEntry == null) return true; // goes to the end of queue
                Long ec = obj.getPriority().priority - obj.getPriority().laxity;
//                LOG.debug("laxityCheck LaxityComparableObject 2. " + obj + " ceiling entry "
//                        + ceilingLaxityEntry+ " ec total " + ecTotal +" ec " + ec + " currentMillis " + currentMillis +
//                        " queue detail " + toString());
                if((ceilingLaxityEntry.getValue() - currentMillis) >= ec){
                    return true;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
//        finally{
//            System.out.println("queue time laxityCheck " + (System.nanoTime() - start)+ " size " + workQueue.size());
//        }
        return false;
    }

    // Test only
    @Override
    public boolean laxityCheck(LaxityComparableObject obj, long baseTime) {
//        Long start = System.nanoTime();
        try{
            Long currentMillis = baseTime;
//            currentMillis = baseTime;
            if(workQueue.size()==0) {
                if(obj.getPriority().laxity < currentMillis ){
                    return false;
                }
                return true;
            }

//            Long ecTotal = workQueue.headMap(obj).entrySet().stream().mapToLong(kv-> {
//                try {
//                    return (kv.getKey().getPriority().priority - kv.getKey().getPriority().laxity);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//                return Long.MAX_VALUE;
//            }).sum();
            // Check the floor key and find execution cost of all previous entry
            Map.Entry<LaxityComparableObject, Long> floorEntry = workQueue.floorEntry(obj);
            Long ecTotal = 0L;
            if(floorEntry != null){
                // original deadline - current laxity = all execution costs
                ecTotal = floorEntry.getKey().getPriority().priority - floorEntry.getValue();
            }
//            LOG.debug("laxityCheck LaxityComparableObject 1. " + obj + " ec total " + ecTotal + " currentMillis " + currentMillis
//                    + " queue size " + workQueue.size() + " queue detail " + toString());
            if(ecTotal + currentMillis <= obj.getPriority().laxity){
                Map.Entry<LaxityComparableObject,Long> ceilingLaxityEntry = laxityMap.ceilingEntry(obj);
                if(ceilingLaxityEntry == null) return true; // goes to the end of queue
                Long ec = obj.getPriority().priority - obj.getPriority().laxity;
//                LOG.debug("laxityCheck LaxityComparableObject 2. " + obj + " ceiling entry "
//                        + ceilingLaxityEntry+ " ec total " + ecTotal +" ec " + ec + " currentMillis " + currentMillis +
//                        " queue detail " + toString());
                if((ceilingLaxityEntry.getValue() - currentMillis) >= ec){
                    return true;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
//        finally{
//            System.out.println("queue time laxityCheck " + (System.nanoTime() - start)+ " size " + workQueue.size());
//        }
        return false;
    }


    @Override
    public LaxityComparableObject poll() {
//        Long start = System.nanoTime();
        LaxityComparableObject ret = null;
        try {
            if(workQueue.size()==0){
                return null;
            }

            ret = workQueue.pollFirstEntry().getKey();
            if(this.laxityMap.containsKey(ret)){
                laxityMap.remove(ret);
            }
            Long ec = ret.getPriority().priority - ret.getPriority().laxity;
            for(Map.Entry<LaxityComparableObject,Long> item : this.workQueue.entrySet()){
                Long updatedLaxity = item.getValue() + ec;
                item.setValue(updatedLaxity);
                if(laxityMap.containsKey(item.getKey())){
                    laxityMap.put(item.getKey(), updatedLaxity);
                }
            }
            return ret;
        } catch (Exception e) {
            LOG.debug("Error polling from queue {}", ret==null? "null":ret.toString());
            e.printStackTrace();
        }
//        finally {
//            System.out.println("queue time poll " + (System.nanoTime() - start) + " size " + workQueue.size());
//        }
        return null;
    }

    @Override
    public LaxityComparableObject peek() {
        return workQueue.firstKey();
    }

    @Override
    public boolean contains(LaxityComparableObject obj) {
        return workQueue.containsKey(obj);
    }

    @Override
    public boolean add(LaxityComparableObject obj) {
//        Long start = System.nanoTime();
        try {
            if(workQueue.size()==0) {
                workQueue.put(obj, obj.getPriority().laxity);
                laxityMap.put(obj, obj.getPriority().laxity);
                return true;
            }
            else{
                Long laxity = obj.getPriority().laxity;
                Long ec = obj.getPriority().priority - obj.getPriority().laxity;
                Long minLaxitySoFar = Long.MAX_VALUE;
                Long minLaxityHeadMap = Long.MAX_VALUE;
                boolean insert = false;
                for(Map.Entry<LaxityComparableObject, Long> item : workQueue.descendingMap().entrySet()){
                    if(item.getKey().compareTo(obj) > 0){
                        Long updatedLaxity = item.getValue() - ec;
                        item.setValue(updatedLaxity);
                        if(minLaxitySoFar > updatedLaxity){
                            minLaxitySoFar = updatedLaxity;
                            laxityMap.put(item.getKey(), updatedLaxity);
                        }
                        if(updatedLaxity < minLaxityHeadMap){
                            minLaxityHeadMap = updatedLaxity;
                        }
                    }
                    else{
                        // guarantee not equal
                        laxity -= (item.getKey().getPriority().priority - item.getKey().getPriority().laxity);
                    }
                }
                if(minLaxityHeadMap > laxity) insert = true;
                SortedMap<LaxityComparableObject, Long> headLaxityMap = laxityMap.headMap(obj);

                Iterator<Map.Entry<LaxityComparableObject, Long>> iter = headLaxityMap.entrySet().iterator();

                while(iter.hasNext()){
                    Map.Entry<LaxityComparableObject, Long> item = iter.next();
                    if(item.getValue() >= laxity) {
                        // remove
                        iter.remove();
//                        if(!insert){
//                            insert = true;
//                        }
                    }
                }
                if(insert) laxityMap.put(obj, laxity);
                workQueue.put(obj, laxity);
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
//        finally {
//            System.out.println("queue time add " + (System.nanoTime() - start)+ " size " + workQueue.size());
//        }
        return false;
    }

    @Override
    public boolean remove(LaxityComparableObject obj) {
//        Long start = System.nanoTime();
        try {
            if(!workQueue.containsKey(obj)) return false;
            Long ec = obj.getPriority().priority - obj.getPriority().laxity;
            boolean landmark = laxityMap.containsKey(obj);
            Long minLaxitySoFar = Long.MAX_VALUE;
            Iterator<Map.Entry<LaxityComparableObject, Long>> iter = workQueue.descendingMap().entrySet().iterator();
            boolean removed = false;
            while(iter.hasNext()){
                Map.Entry<LaxityComparableObject, Long> item = iter.next();
                if(item.getKey().compareTo(obj) > 0){
                    // Right map, all laxity increases
                    Long updatedLaxity = item.getValue() + ec;
                    item.setValue(updatedLaxity);
                    if(laxityMap.containsKey(item.getKey())){
                        laxityMap.put(item.getKey(), updatedLaxity);
                    }
                    if(updatedLaxity < minLaxitySoFar) minLaxitySoFar = updatedLaxity;
                }
                else if (item.getKey().compareTo(obj) == 0){
                    if(laxityMap.containsKey(obj)){
                        Long v = laxityMap.remove(obj);
                    }
                    iter.remove();
                    removed = true;
                }
                else{
                    // Left map, upgrade if remove landmark item
                    if(!landmark) break;
                    long currentLaxity = item.getValue();
                    if(currentLaxity < minLaxitySoFar) {
                        minLaxitySoFar = currentLaxity;
                        laxityMap.put(item.getKey(), currentLaxity);
                    }
                }
            }
            return removed;
        } catch (Exception e) {
            e.printStackTrace();
        }
//        finally{
//            System.out.println("queue time remove " + (System.nanoTime() - start)+ " size " + workQueue.size());
//        }
        return false;
    }

    @Override
    public int size() {
        return workQueue.size();
    }

    @Override
    public Set<LaxityComparableObject> tailSet(LaxityComparableObject obj) {
        return workQueue.tailMap(obj).keySet();
    }

    @Override
    public WorkQueue<LaxityComparableObject> copy() {
        return new PriorityBasedMinLaxityWorkQueue<>(workQueue, laxityMap);
    }

    @Override
    public Iterable<LaxityComparableObject> toIterable() {
        return workQueue.keySet();
    }

    @Override
    public String dumpLaxityMap() {
        return String.format("LaxityMap: " + laxityMap.entrySet().stream()
                .map(kv -> kv.getKey() + " -> " + kv.getValue())
                .collect(Collectors.joining(",")));
    }

    @Override
    public String toString(){
        return String.format("PriorityBasedMinLaxityWorkQueue: priority queue [%s] laxityMap [%s]",
                Arrays.toString(workQueue.entrySet().stream().map(kv-> kv.getKey() + " -> " + kv.getValue()+ "\n").toArray() ),
                Arrays.toString(laxityMap.entrySet().stream().map(kv -> kv.getKey() + " -> " + kv.getValue()+ "\n").toArray()));
    }
}


//public class PriorityBasedMinLaxityWorkQueue<T extends LaxityComparableObject> extends MinLaxityWorkQueue<LaxityComparableObject> {
//
//    TreeMap<LaxityComparableObject, Long> workQueue;
//    TreeMap<LaxityComparableObject, Long> laxityMap;
//    private static final Logger LOG = LoggerFactory.getLogger(FunctionActivation.class);
//
//    public PriorityBasedMinLaxityWorkQueue(){
//        this.workQueue = new TreeMap<>();
//        this.laxityMap = new TreeMap<>();
//    }
//
//    public PriorityBasedMinLaxityWorkQueue(TreeMap<LaxityComparableObject, Long> workQueue,
//                                           TreeMap<LaxityComparableObject, Long> laxityMap){
//        this.workQueue = new TreeMap<>(workQueue);
//        this.laxityMap = new TreeMap<>(laxityMap);
//    }
//
//    @Override
//    public boolean tryInsertWithLaxityCheck(LaxityComparableObject obj) {
//        Long start = System.nanoTime();
//        try {
//            if(workQueue.size()==0) {
//                Long currentMillis = System.currentTimeMillis();
//                if(obj.getPriority().laxity < currentMillis ){
//                    return false;
//                }
//                workQueue.put(obj, obj.getPriority().laxity);
//                laxityMap.put(obj, obj.getPriority().laxity);
//                return true;
//            }
//            else{
//                Long currentMillis = System.currentTimeMillis();
//                Long laxity = obj.getPriority().laxity;
//                Long ec = obj.getPriority().priority - obj.getPriority().laxity;
//                Long minLaxitySoFar = Long.MAX_VALUE;
//                Long minLaxityHeadMap = Long.MAX_VALUE;
//                boolean preCheck = false;
//                boolean insert = false;
//                for(Map.Entry<LaxityComparableObject, Long> item : workQueue.entrySet()){
//                    if(item.getKey().compareTo(obj) > 0){
//                        if(!preCheck){
//                            // check before touch laxity stored
//                            Map.Entry<LaxityComparableObject, Long> nextItem = laxityMap.ceilingEntry(obj);
//                            if(nextItem!=null && (nextItem.getValue() - ec < currentMillis)){
//                                return false;
//                            }
//                            preCheck = true;
//                        }
//                        Long updatedLaxity = item.getValue() - ec;
//                        item.setValue(updatedLaxity);
//                        if(minLaxitySoFar > updatedLaxity){
//                            minLaxitySoFar = updatedLaxity;
//                            laxityMap.put(item.getKey(), updatedLaxity);
//                        }
//                        if(updatedLaxity < minLaxityHeadMap){
//                            minLaxityHeadMap = updatedLaxity;
//                        }
//                    }
//                    else{
//                        // guarantee not equal
//                        laxity -= (item.getKey().getPriority().priority - item.getKey().getPriority().laxity);
//                        if(laxity < currentMillis) return false;
//                    }
//                }
//                if(minLaxityHeadMap > laxity) insert = true;
//                SortedMap<LaxityComparableObject, Long> headLaxityMap = laxityMap.headMap(obj);
//
//                Iterator<Map.Entry<LaxityComparableObject, Long>> iter = headLaxityMap.entrySet().iterator();
//
//                while(iter.hasNext()){
//                    Map.Entry<LaxityComparableObject, Long> item = iter.next();
//                    if(item.getValue() >= laxity) {
//                        // remove
//                        iter.remove();
////                        if(!insert){
////                            insert = true;
////                        }
//                    }
//                }
//                if(insert) laxityMap.put(obj, laxity);
//                workQueue.put(obj, laxity);
//                return true;
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        finally {
//            System.out.println("queue time tryInsertWithLaxityCheck " + (System.nanoTime() - start) + " size " + workQueue.size());
//        }
//        return false;
//    }
//
//
//    // Test only
//    public boolean tryInsertWithLaxityCheck(LaxityComparableObject obj, long baseTime) {
//        Long start = System.nanoTime();
//        try {
//            if(workQueue.size()==0) {
//                Long currentMillis = System.currentTimeMillis();
//                currentMillis = baseTime;
//                if(obj.getPriority().laxity < currentMillis ){
//                    return false;
//                }
//                workQueue.put(obj, obj.getPriority().laxity);
//                laxityMap.put(obj, obj.getPriority().laxity);
//                return true;
//            }
//            else{
//                Long currentMillis = System.currentTimeMillis();
//                currentMillis = baseTime;
//                Long laxity = obj.getPriority().laxity;
//                Long ec = obj.getPriority().priority - obj.getPriority().laxity;
//                Long minLaxitySoFar = Long.MAX_VALUE;
//                Long minLaxityHeadMap = Long.MAX_VALUE;
//                boolean preCheck = false;
//                boolean insert = false;
//                for(Map.Entry<LaxityComparableObject, Long> item : workQueue.entrySet()){
//                    if(item.getKey().compareTo(obj) > 0){
//                        if(!preCheck){
//                            // check before touch laxity stored
//                            Map.Entry<LaxityComparableObject, Long> nextItem = laxityMap.ceilingEntry(obj);
//                            if(nextItem!=null && (nextItem.getValue() - ec < currentMillis)){
//                                return false;
//                            }
//                            preCheck = true;
//                        }
//                        Long updatedLaxity = item.getValue() - ec;
//                        item.setValue(updatedLaxity);
//                        if(minLaxitySoFar > updatedLaxity){
//                            minLaxitySoFar = updatedLaxity;
//                            laxityMap.put(item.getKey(), updatedLaxity);
//                        }
//                        if(updatedLaxity < minLaxityHeadMap){
//                            minLaxityHeadMap = updatedLaxity;
//                        }
//                    }
//                    else{
//                        // guarantee not equal
//                        laxity -= (item.getKey().getPriority().priority - item.getKey().getPriority().laxity);
//                        if(laxity < currentMillis) return false;
//                    }
//                }
//                if(minLaxityHeadMap > laxity) insert = true;
//                SortedMap<LaxityComparableObject, Long> headLaxityMap = laxityMap.headMap(obj);
//
//                Iterator<Map.Entry<LaxityComparableObject, Long>> iter = headLaxityMap.entrySet().iterator();
//
//                while(iter.hasNext()){
//                    Map.Entry<LaxityComparableObject, Long> item = iter.next();
//                    if(item.getValue() >= laxity) {
//                        // remove
//                        iter.remove();
////                        if(!insert){
////                            insert = true;
////                        }
//                    }
//                }
//                if(insert) laxityMap.put(obj, laxity);
//                workQueue.put(obj, laxity);
//                return true;
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        finally {
//            System.out.println("queue time tryInsertWithLaxityCheck " + (System.nanoTime() - start) + " size " + workQueue.size());
//        }
//        return false;
//    }
//
//    @Override
//    public boolean laxityCheck(LaxityComparableObject obj) {
//        Long start = System.nanoTime();
//        try{
//            Long currentMillis = System.currentTimeMillis();
//            if(workQueue.size()==0) {
//                if(obj.getPriority().laxity < currentMillis ){
//                    return false;
//                }
//                return true;
//            }
//
//            Long ecTotal = workQueue.headMap(obj).entrySet().stream().mapToLong(kv-> {
//                try {
//                    return (kv.getKey().getPriority().priority - kv.getKey().getPriority().laxity);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//                return Long.MAX_VALUE;
//            }).sum();
////            LOG.debug("laxityCheck LaxityComparableObject 1. " + obj + " ec total " + ecTotal + " currentMillis " + currentMillis
////                    + " queue size " + workQueue.size() + " queue detail " + toString());
//            if(ecTotal + currentMillis < obj.getPriority().laxity){
//                Map.Entry<LaxityComparableObject,Long> ceilingLaxityEntry = laxityMap.ceilingEntry(obj);
//                if(ceilingLaxityEntry == null) return true; // goes to the end of queue
//                Long ec = obj.getPriority().priority - obj.getPriority().laxity;
////                LOG.debug("laxityCheck LaxityComparableObject 2. " + obj + " ceiling entry "
////                        + ceilingLaxityEntry+ " ec total " + ecTotal +" ec " + ec + " currentMillis " + currentMillis +
////                        " queue detail " + toString());
//                if((ceilingLaxityEntry.getValue() - currentMillis) >= ec){
//                    return true;
//                }
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        finally{
//            System.out.println("queue time laxityCheck " + (System.nanoTime() - start)+ " size " + workQueue.size());
//        }
//        return false;
//    }
//
//    // Test only
//    public boolean laxityCheck(LaxityComparableObject obj, long baseTime) {
//        Long start = System.nanoTime();
//        try{
//            Long currentMillis = System.currentTimeMillis();
//            currentMillis = baseTime;
//            if(workQueue.size()==0) {
//                if(obj.getPriority().laxity < currentMillis ){
//                    return false;
//                }
//                return true;
//            }
//
//            Long ecTotal = workQueue.headMap(obj).entrySet().stream().mapToLong(kv-> {
//                try {
//                    return (kv.getKey().getPriority().priority - kv.getKey().getPriority().laxity);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//                return Long.MAX_VALUE;
//            }).sum();
////            LOG.debug("laxityCheck LaxityComparableObject 1. " + obj + " ec total " + ecTotal + " currentMillis " + currentMillis
////                    + " queue size " + workQueue.size() + " queue detail " + toString());
//            if(ecTotal + currentMillis < obj.getPriority().laxity){
//                Map.Entry<LaxityComparableObject,Long> ceilingLaxityEntry = laxityMap.ceilingEntry(obj);
//                if(ceilingLaxityEntry == null) return true; // goes to the end of queue
//                Long ec = obj.getPriority().priority - obj.getPriority().laxity;
////                LOG.debug("laxityCheck LaxityComparableObject 2. " + obj + " ceiling entry "
////                        + ceilingLaxityEntry+ " ec total " + ecTotal +" ec " + ec + " currentMillis " + currentMillis +
////                        " queue detail " + toString());
//                if((ceilingLaxityEntry.getValue() - currentMillis) >= ec){
//                    return true;
//                }
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        finally{
//            System.out.println("queue time laxityCheck " + (System.nanoTime() - start)+ " size " + workQueue.size());
//        }
//        return false;
//    }
//
//
//    @Override
//    public LaxityComparableObject poll() {
//        Long start = System.nanoTime();
//        LaxityComparableObject ret = null;
//        try {
//            if(workQueue.size()==0){
//                return null;
//            }
//
//            ret = workQueue.pollFirstEntry().getKey();
//            if(this.laxityMap.containsKey(ret)){
//                laxityMap.remove(ret);
//            }
//            Long ec = ret.getPriority().priority - ret.getPriority().laxity;
//            for(Map.Entry<LaxityComparableObject,Long> item : this.workQueue.entrySet()){
//                Long updatedLaxity = item.getValue() + ec;
//                item.setValue(updatedLaxity);
//                if(laxityMap.containsKey(item.getKey())){
//                    laxityMap.put(item.getKey(), updatedLaxity);
//                }
//            }
//            return ret;
//        } catch (Exception e) {
//            LOG.debug("Error polling from queue {}", ret==null? "null":ret.toString());
//            e.printStackTrace();
//        }
//        finally {
//            System.out.println("queue time poll " + (System.nanoTime() - start) + " size " + workQueue.size());
//        }
//        return null;
//    }
//
//    @Override
//    public LaxityComparableObject peek() {
//        return workQueue.firstKey();
//    }
//
//    @Override
//    public boolean contains(LaxityComparableObject obj) {
//        return workQueue.containsKey(obj);
//    }
//
//    @Override
//    public boolean add(LaxityComparableObject obj) {
//        Long start = System.nanoTime();
//        try {
//            if(workQueue.size()==0) {
//                workQueue.put(obj, obj.getPriority().laxity);
//                laxityMap.put(obj, obj.getPriority().laxity);
//                return true;
//            }
//            else{
//                Long laxity = obj.getPriority().laxity;
//                Long ec = obj.getPriority().priority - obj.getPriority().laxity;
//                Long minLaxitySoFar = Long.MAX_VALUE;
//                Long minLaxityHeadMap = Long.MAX_VALUE;
//                boolean insert = false;
//                for(Map.Entry<LaxityComparableObject, Long> item : workQueue.descendingMap().entrySet()){
//                    if(item.getKey().compareTo(obj) > 0){
//                        Long updatedLaxity = item.getValue() - ec;
//                        item.setValue(updatedLaxity);
//                        if(minLaxitySoFar > updatedLaxity){
//                            minLaxitySoFar = updatedLaxity;
//                            laxityMap.put(item.getKey(), updatedLaxity);
//                        }
//                        if(updatedLaxity < minLaxityHeadMap){
//                            minLaxityHeadMap = updatedLaxity;
//                        }
//                    }
//                    else{
//                        // guarantee not equal
//                        laxity -= (item.getKey().getPriority().priority - item.getKey().getPriority().laxity);
//                    }
//                }
//                if(minLaxityHeadMap > laxity) insert = true;
//                SortedMap<LaxityComparableObject, Long> headLaxityMap = laxityMap.headMap(obj);
//
//                Iterator<Map.Entry<LaxityComparableObject, Long>> iter = headLaxityMap.entrySet().iterator();
//
//                while(iter.hasNext()){
//                    Map.Entry<LaxityComparableObject, Long> item = iter.next();
//                    if(item.getValue() >= laxity) {
//                        // remove
//                        iter.remove();
////                        if(!insert){
////                            insert = true;
////                        }
//                    }
//                }
//                if(insert) laxityMap.put(obj, laxity);
//                workQueue.put(obj, laxity);
//                return true;
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            System.out.println("queue time add " + (System.nanoTime() - start)+ " size " + workQueue.size());
//        }
//        return false;
//    }
//
//    @Override
//    public boolean remove(LaxityComparableObject obj) {
//        Long start = System.nanoTime();
//        try {
//            if(!workQueue.containsKey(obj)) return false;
//            Long ec = obj.getPriority().priority - obj.getPriority().laxity;
//            boolean landmark = laxityMap.containsKey(obj);
//            Long minLaxitySoFar = Long.MAX_VALUE;
//            Iterator<Map.Entry<LaxityComparableObject, Long>> iter = workQueue.descendingMap().entrySet().iterator();
//            boolean removed = false;
//            while(iter.hasNext()){
//                Map.Entry<LaxityComparableObject, Long> item = iter.next();
//                if(item.getKey().compareTo(obj) > 0){
//                    // Right map, all laxity increases
//                    Long updatedLaxity = item.getValue() + ec;
//                    item.setValue(updatedLaxity);
//                    if(laxityMap.containsKey(item.getKey())){
//                        laxityMap.put(item.getKey(), updatedLaxity);
//                    }
//                    if(updatedLaxity < minLaxitySoFar) minLaxitySoFar = updatedLaxity;
//                }
//                else if (item.getKey().compareTo(obj) == 0){
//                    if(laxityMap.containsKey(obj)){
//                        Long v = laxityMap.remove(obj);
//                    }
//                    iter.remove();
//                    removed = true;
//                }
//                else{
//                    // Left map, upgrade if remove landmark item
//                    if(!landmark) break;
//                    long currentLaxity = item.getValue();
//                    if(currentLaxity < minLaxitySoFar) {
//                        minLaxitySoFar = currentLaxity;
//                        laxityMap.put(item.getKey(), currentLaxity);
//                    }
//                }
//            }
//            return removed;
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        finally{
//            System.out.println("queue time remove " + (System.nanoTime() - start)+ " size " + workQueue.size());
//        }
//        return false;
//    }
//
//    @Override
//    public int size() {
//        return workQueue.size();
//    }
//
//    @Override
//    public Set<LaxityComparableObject> tailSet(LaxityComparableObject obj) {
//        return workQueue.tailMap(obj).keySet();
//    }
//
//    @Override
//    public WorkQueue<LaxityComparableObject> copy() {
//        return new PriorityBasedMinLaxityWorkQueue<>(workQueue, laxityMap);
//    }
//
//    @Override
//    public Iterable<LaxityComparableObject> toIterable() {
//        return workQueue.keySet();
//    }
//
//    @Override
//    public String dumpLaxityMap() {
//        return String.format("LaxityMap: " + laxityMap.entrySet().stream()
//                .map(kv -> kv.getKey() + " -> " + kv.getValue())
//                .collect(Collectors.joining(",")));
//    }
//
//    @Override
//    public String toString(){
//        return String.format("PriorityBasedMinLaxityWorkQueue: priority queue [%s] laxityMap [%s]",
//                Arrays.toString(workQueue.entrySet().stream().map(kv-> kv.getKey() + " -> " + kv.getValue()).toArray()),
//                Arrays.toString(laxityMap.entrySet().stream().map(kv -> kv.getKey() + " -> " + kv.getValue()).toArray()));
//    }
//}

/*
public class PriorityBasedMinLaxityWorkQueue<T extends LaxityComparableObject> extends MinLaxityWorkQueue<LaxityComparableObject> {
    PriorityQueue<LaxityComparableObject> workQueue;
    TreeMap<LaxityComparableObject, Long> laxityMap;


    @Override
    public boolean tryInsertWithLaxityCheck(LaxityComparableObject obj) {
        try{
            Long currentMillis = System.currentTimeMillis();
            Map.Entry<LaxityComparableObject,Long> floorLaxityEntry = laxityMap.floorEntry(obj);
            PriorityQueue<LaxityComparableObject> workQueueCopy = new PriorityQueue<>(workQueue);
            Long ecTotal = workQueueCopy.stream().filter(x->(x.compareTo(obj)<0)).mapToLong(x-> {
                try {
                    return (x.getPriority().priority - x.getPriority().laxity);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return Long.MAX_VALUE;
            }).sum();
            if(ecTotal + currentMillis < obj.getPriority().laxity){
                if(floorLaxityEntry == null) return add(obj);
                Long ec = obj.getPriority().priority - obj.getPriority().laxity;
                if((floorLaxityEntry.getValue() - currentMillis) >= ec){
                    return add(obj);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean laxityCheck(LaxityComparableObject obj) {
        try{
            Long currentMillis = System.currentTimeMillis();
            Map.Entry<LaxityComparableObject,Long> floorLaxityEntry = laxityMap.floorEntry(obj);
            PriorityQueue<LaxityComparableObject> workQueueCopy = new PriorityQueue<>(workQueue);
            Long ecTotal = workQueueCopy.stream().filter(x->(x.compareTo(obj)<0)).mapToLong(x-> {
                try {
                    return (x.getPriority().priority - x.getPriority().laxity);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return Long.MAX_VALUE;
            }).sum();
            if(ecTotal + currentMillis < obj.getPriority().laxity){
                if(floorLaxityEntry == null) return true;
                Long ec = obj.getPriority().priority - obj.getPriority().laxity;
                if((floorLaxityEntry.getValue() - currentMillis) >= ec){
                    return true;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public String dumpLaxityMap() {
        return String.format("LaxityMap: " + laxityMap.entrySet().stream()
                .map(kv -> kv.getKey() + " -> " + kv.getValue())
                .collect(Collectors.joining(",")));
    }

    public PriorityBasedMinLaxityWorkQueue(){
        this.workQueue = new PriorityQueue<>();
        this.laxityMap = new TreeMap<>(Comparator.reverseOrder());
    }

    public PriorityBasedMinLaxityWorkQueue(PriorityQueue<LaxityComparableObject> workQueue,
                                           TreeMap<LaxityComparableObject, Long> laxityMap){
        this.workQueue = new PriorityQueue<>(workQueue);
        this.laxityMap = new TreeMap<>(laxityMap);
    }

    @Override
    public LaxityComparableObject poll() {
        try {
            LaxityComparableObject ret = workQueue.poll();
            if(ret == null){
                return ret;
            }
            if(this.laxityMap.containsKey(ret)){
                laxityMap.remove(ret);
            }
            Long ec = ret.getPriority().priority - ret.getPriority().laxity;
            for(Map.Entry<LaxityComparableObject,Long> e : laxityMap.entrySet()){
                laxityMap.put(e.getKey(), e.getValue() + ec);
            }
            return ret;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public boolean contains(LaxityComparableObject obj) {
        return workQueue.contains(obj);
    }

    @Override
    public boolean add(LaxityComparableObject obj) {
        try {
            if(laxityMap.isEmpty()){
                laxityMap.put(obj, obj.getPriority().laxity);
            }
            else {
                int iterCount = 0;
                HashMap<LaxityComparableObject, Long> toAdd = new HashMap<>();
                HashSet<LaxityComparableObject> toRemove = new HashSet<>();
                boolean inserted = false;
                Long lastSeenLaxity = 0L;
                Long laxity = obj.getPriority().priority;
                Long ec = obj.getPriority().priority - laxity;
                for (Map.Entry<LaxityComparableObject, Long> kv : laxityMap.entrySet()) {
                    Long currentLaxity = kv.getValue();
                    if (kv.getKey().compareTo(obj) > 0) {
                        //reduce laxity for items on the right
                        currentLaxity-=ec;
                        if(currentLaxity <= laxity){
                            inserted = true;
                        }
                        laxityMap.put(kv.getKey(), currentLaxity);
                        if (iterCount == laxityMap.size() - 1) {
                            // being the mimimum item
                            laxityMap.put(obj, obj.getPriority().laxity);
                            break;
                        }
                    } else {
                        if (laxity.equals(currentLaxity) ) {
                            // replace and becomes new boundary
                            if(!inserted){
                                toAdd.put(obj, laxity);
                                toRemove.add(kv.getKey());
                                inserted = true;
                            }
                        } else if (laxity < currentLaxity) {
                            // update greater laxity on the left becomes laxity
                            toRemove.add(kv.getKey());
                        } else {
                            // greater than the minimum on the right and has no effect to the rest
                            if((iterCount==0 || !laxity.equals(lastSeenLaxity)) && !inserted) {
                                laxityMap.put(obj, laxity);
                                inserted = true;
                            }
                            break;
                        }
                    }
                    iterCount++;
                    lastSeenLaxity = currentLaxity;
                    if(currentLaxity < laxity){
                        laxity = currentLaxity;
                    }
                }
                for (LaxityComparableObject item : toRemove) {
                    laxityMap.remove(item);
                }
                for (Map.Entry<LaxityComparableObject, Long> item : toAdd.entrySet()) {
                    laxityMap.put(item.getKey(), item.getValue());
                }
            }
            return workQueue.add(obj);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean remove(LaxityComparableObject obj) {
        SortedMap<LaxityComparableObject,Long> laxityHeadMap = laxityMap.headMap(obj, false);
        Iterator<LaxityComparableObject> iter = workQueue.iterator();
        PriorityQueue<LaxityComparableObject> workQueueCopy = new PriorityQueue<>(workQueue);
        LaxityComparableObject lastEntry = null;
        boolean removed = false;
        try {
        LaxityComparableObject item = workQueueCopy.poll();
        while(item !=null){
            if(item.equals(obj)){
                workQueue.remove(item);
                removed = true;
                if(laxityMap.containsKey(item)){
                    Long laxity = laxityMap.remove(item);
                    if(lastEntry!=null && !laxityMap.containsKey(lastEntry)){
                        laxityMap.put(lastEntry, laxity);
                    }
                }
                Long ec = null;

                ec = obj.getPriority().priority - obj.getPriority().laxity;

                for(Map.Entry<LaxityComparableObject, Long> e : laxityHeadMap.entrySet()){
                    laxityMap.put(e.getKey(), e.getValue() + ec);
                }
                break;
            }
            lastEntry = item;
            item = workQueueCopy.poll();
        }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return removed;
    }

    @Override
    public int size() {
        return workQueue.size();
    }

    @Override
    public SortedSet<LaxityComparableObject> tailSet(LaxityComparableObject obj) {
        PriorityBasedUnsafeWorkQueue<LaxityComparableObject> duplicate = new PriorityBasedUnsafeWorkQueue<>(workQueue);
        SortedSet<LaxityComparableObject> ret = new TreeSet<>();
        while(duplicate.size()>0){
            LaxityComparableObject polled = duplicate.poll();
            if(polled.compareTo(obj)<=0){
                continue;
            }
            ret.add(polled);
        }
        return ret;
    }

    @Override
    public WorkQueue<LaxityComparableObject> copy() {
        return new PriorityBasedMinLaxityWorkQueue<>(workQueue, laxityMap);
    }

    @Override
    public String toString(){
        PriorityQueue<LaxityComparableObject> copy = new PriorityQueue<>(workQueue);
        ArrayList<LaxityComparableObject> queue = new ArrayList<>();
        while(!copy.isEmpty()){
            queue.add(copy.poll());
        }
        return String.format("PriorityBasedMinLaxityWorkQueue: priority queue [%s] laxityMap [%s]",
                Arrays.toString(queue.toArray()),
                Arrays.toString(laxityMap.entrySet().stream().map(kv -> kv.getKey() + " -> " + kv.getValue()).toArray()));
    }
}
*/