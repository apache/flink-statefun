package org.apache.flink.statefun.flink.core.functions.utils;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class PriorityBasedDefaultLaxityWorkQueue <T extends LaxityComparableObject> extends MinLaxityWorkQueue<LaxityComparableObject>{
    TreeMap<LaxityComparableObject, Long> workQueue;

    public PriorityBasedDefaultLaxityWorkQueue(){
        this.workQueue = new TreeMap<>();
    }

    public PriorityBasedDefaultLaxityWorkQueue(TreeMap<LaxityComparableObject, Long> workQueue){
        this.workQueue = new TreeMap<>(workQueue);
    }

    @Override
    public boolean tryInsertWithLaxityCheck(LaxityComparableObject item) {
//        Long start = System.nanoTime();
        Long ecTotal = 0L;
        Long curTime = System.currentTimeMillis();
        try {

            ecTotal = workQueue.headMap(item).entrySet().stream().mapToLong(kv-> {
                try {
                    return (kv.getKey().getPriority().priority - kv.getKey().getPriority().laxity);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return Long.MAX_VALUE;
            }).sum();
            if(ecTotal + curTime <= item.getPriority().laxity){
                Set<Map.Entry<LaxityComparableObject, Long>> tailMap = workQueue.tailMap(item, false).entrySet();
                ecTotal += (item.getPriority().priority - item.getPriority().laxity); // add ec of item first
                for(Map.Entry<LaxityComparableObject, Long> e : tailMap){
                    if (e.getKey().getPriority().laxity < curTime + ecTotal){
                        return false;
                    }
                    // accumulate execution cost
                    ecTotal += e.getKey().getPriority().priority - e.getKey().getPriority().laxity;
                }
                workQueue.put(item, item.getPriority().laxity);
                return true;
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
//        finally {
//            System.out.println("queue time tryInsertWithLaxityCheck " + (System.nanoTime() - start) + " size " + workQueue.size());
//        }

        return false;
    }

    //Test Only
    @Override
    public boolean tryInsertWithLaxityCheck(LaxityComparableObject item, long baseTime) {
//        Long start = System.nanoTime();
        Long ecTotal = 0L;
        Long curTime = baseTime;
        try {

            ecTotal = workQueue.headMap(item).entrySet().stream().mapToLong(kv-> {
                try {
                    return (kv.getKey().getPriority().priority - kv.getKey().getPriority().laxity);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return Long.MAX_VALUE;
            }).sum();
            if(ecTotal + curTime <= item.getPriority().laxity){
                Set<Map.Entry<LaxityComparableObject, Long>> tailMap = workQueue.tailMap(item, false).entrySet();
                ecTotal += (item.getPriority().priority - item.getPriority().laxity); // add ec of item first
                for(Map.Entry<LaxityComparableObject, Long> e : tailMap){
                    if (e.getKey().getPriority().laxity < curTime + ecTotal){
                        return false;
                    }
                    // accumulate execution cost
                    ecTotal += e.getKey().getPriority().priority - e.getKey().getPriority().laxity;
                }
                workQueue.put(item, item.getPriority().laxity);
                return true;
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
//        finally {
//            System.out.println("queue time tryInsertWithLaxityCheck " + (System.nanoTime() - start) + " size " + workQueue.size());
//        }

        return false;
    }

    @Override
    public boolean laxityCheck(LaxityComparableObject item) {
        Long ecTotal = 0L;
//        Long start = System.nanoTime();
        Long curTime = System.currentTimeMillis();
        try {

            ecTotal = workQueue.headMap(item).entrySet().stream().mapToLong(kv-> {
                try {
                    return (kv.getKey().getPriority().priority - kv.getKey().getPriority().laxity);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return Long.MAX_VALUE;
            }).sum();
            if(ecTotal + curTime <= item.getPriority().laxity){
                Set<Map.Entry<LaxityComparableObject, Long>> tailMap = workQueue.tailMap(item, false).entrySet();
                ecTotal += (item.getPriority().priority - item.getPriority().laxity); // add ec of item first
                for(Map.Entry<LaxityComparableObject, Long> e : tailMap){
                    if (e.getKey().getPriority().laxity < curTime + ecTotal){
                        return false;
                    }
                    // accumulate execution cost
                    ecTotal += e.getKey().getPriority().priority - e.getKey().getPriority().laxity;
                }
                return true;
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
//        finally{
//            System.out.println("queue time laxityCheck " + (System.nanoTime() - start)+ " size " + workQueue.size());
//        }
        return false;
    }

    // Test only
    @Override
    public boolean laxityCheck(LaxityComparableObject item, long baseTime) {
        Long ecTotal = 0L;
//        Long start = System.nanoTime();
        Long curTime = baseTime;
        try {
            ecTotal = workQueue.headMap(item).entrySet().stream().mapToLong(kv-> {
                try {
                    return (kv.getKey().getPriority().priority - kv.getKey().getPriority().laxity);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return Long.MAX_VALUE;
            }).sum();
            if(ecTotal + curTime <= item.getPriority().laxity){
                Set<Map.Entry<LaxityComparableObject, Long>> tailMap = workQueue.tailMap(item, false).entrySet();
                ecTotal += (item.getPriority().priority - item.getPriority().laxity); // add ec of item first
                for(Map.Entry<LaxityComparableObject, Long> e : tailMap){
                    if (e.getKey().getPriority().laxity < curTime + ecTotal){
                        return false;
                    }
                    // accumulate execution cost
                    ecTotal += e.getKey().getPriority().priority - e.getKey().getPriority().laxity;
                }
                return true;
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
//        finally{
//            System.out.println("queue time laxityCheck " + (System.nanoTime() - start)+ " size " + workQueue.size());
//        }
        return false;
    }

    @Override
    public String dumpLaxityMap() {
        return "No Laxity Map";
    }

    @Override
    public LaxityComparableObject poll() {
//        Long start = System.nanoTime();
//        try{
            Map.Entry<LaxityComparableObject, Long> first = workQueue.pollFirstEntry();
            return first==null? null: first.getKey();
//        }
//        finally {
//            System.out.println("queue time poll " + (System.nanoTime() - start) + " size " + workQueue.size());
//        }
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
            workQueue.put(obj, obj.getPriority().laxity);
        } catch (Exception e) {
            e.printStackTrace();
        }
//        finally {
//            System.out.println("queue time add " + (System.nanoTime() - start)+ " size " + workQueue.size());
//        }
        return true;
    }

    @Override
    public boolean remove(LaxityComparableObject obj) {
//        Long start = System.nanoTime();
//        try{
            return workQueue.remove(obj) != null;
//        }
//        finally{
//            System.out.println("queue time remove " + (System.nanoTime() - start)+ " size " + workQueue.size());
//        }

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
        return new PriorityBasedDefaultLaxityWorkQueue(workQueue);
    }

    @Override
    public Iterable<LaxityComparableObject> toIterable() {
        return workQueue.keySet();
    }

    @Override
    public String toString(){
        return String.format("PriorityBasedDefaultLaxityWorkQueue: priority queue \n [%s]",
                Arrays.toString(workQueue.entrySet().stream().map(kv-> kv.getKey() + " -> " + kv.getValue()+ "\n").toArray()));
    }
}
