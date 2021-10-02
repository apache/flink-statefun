package org.apache.flink.statefun.flink.core.functions;

import org.apache.flink.statefun.flink.core.functions.utils.LaxityComparableObject;
import org.apache.flink.statefun.flink.core.functions.utils.MinLaxityWorkQueue;
import org.apache.flink.statefun.flink.core.functions.utils.PriorityBasedDefaultLaxityWorkQueue;
import org.apache.flink.statefun.flink.core.functions.utils.PriorityBasedMinLaxityWorkQueue;
import org.apache.flink.statefun.flink.core.message.PriorityObject;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

public class LaxityWorkQueueTest {

    private MinLaxityWorkQueue<testLaxityObject> queueMin;
    private MinLaxityWorkQueue<testLaxityObject> queueDefault;

    @Before
    public void setUp() {
        queueMin = new PriorityBasedMinLaxityWorkQueue();
        queueDefault = new PriorityBasedDefaultLaxityWorkQueue();
    }

    void testInsertQueue(MinLaxityWorkQueue<testLaxityObject> queue){
        ArrayList<testLaxityObject> items = new ArrayList<>();
        items.add(new testLaxityObject(new PriorityObject(1L, 0L)));
        items.add(new testLaxityObject(new PriorityObject(12L, 11L)));
        items.add(new testLaxityObject(new PriorityObject(13L, 12L)));
        items.add(new testLaxityObject(new PriorityObject(3L, 2L)));
        items.add(new testLaxityObject(new PriorityObject(7L, 6L)));
        items.add(new testLaxityObject(new PriorityObject(3L, 2L)));
        items.add(new testLaxityObject(new PriorityObject(7L, 6L)));
        items.add(new testLaxityObject(new PriorityObject(5L, 4L)));
        items.add(new testLaxityObject(new PriorityObject(6L, 3L)));


        queue.add(items.get(0));
        System.out.println("1, 0");
        System.out.println(queue.toString());
        queue.add(items.get(1));
        System.out.println("1, 0");
        System.out.println(queue.toString());
        queue.add(items.get(2));
        System.out.println("13, 12");
        System.out.println(queue.toString());
        queue.add(items.get(3));
        System.out.println("3, 2");
        System.out.println(queue.toString());
        queue.add(items.get(4));
        System.out.println("7, 6");
        System.out.println(queue.toString());
        queue.add(items.get(5));
        System.out.println("3, 2");
        System.out.println(queue.toString());
        queue.add(items.get(6));
        System.out.println("7, 6");
        System.out.println(queue.toString());
        queue.add(items.get(7));
        System.out.println("5, 4");
        System.out.println(queue.toString());
        queue.add(items.get(8));
        System.out.println("6, 3");
        System.out.println(queue.toString());


        queue.poll();
        System.out.println("poll");
        System.out.println(queue.toString());

        queue.remove(items.get(6));
        System.out.println("remove 6th");
        System.out.println(queue.toString());

        queue.remove(items.get(4));
        System.out.println("remove 4th");
        System.out.println(queue.toString());

        queue.remove(items.get(8));
        System.out.println("remove 8th");
        System.out.println(queue.toString());

        queue.remove(items.get(5));
        System.out.println("remove 5th");
        System.out.println(queue.toString());
    }

    public void testInsertCheckQueue(MinLaxityWorkQueue<testLaxityObject> queue){

        ArrayList<testLaxityObject> items = new ArrayList<>();
        items.add(new testLaxityObject(new PriorityObject(1L, 0L)));
        items.add(new testLaxityObject(new PriorityObject(12L, 11L)));
        items.add(new testLaxityObject(new PriorityObject(13L, 12L)));
        items.add(new testLaxityObject(new PriorityObject(3L, 2L)));
        items.add(new testLaxityObject(new PriorityObject(7L, 6L)));
        items.add(new testLaxityObject(new PriorityObject(3L, 2L)));
        items.add(new testLaxityObject(new PriorityObject(7L, 6L)));
        items.add(new testLaxityObject(new PriorityObject(5L, 4L)));
        items.add(new testLaxityObject(new PriorityObject(6L, 3L)));


        queue.add(items.get(0));
        System.out.println("1, 0");
        System.out.println(queue.toString());
        queue.add(items.get(1));
        System.out.println("1, 0");
        System.out.println(queue.toString());
        queue.add(items.get(2));
        System.out.println("13, 12");
        System.out.println(queue.toString());
        queue.add(items.get(3));
        System.out.println("3, 2");
        System.out.println(queue.toString());
        queue.add(items.get(4));
        System.out.println("7, 6");
        System.out.println(queue.toString());
        queue.add(items.get(5));
        System.out.println("3, 2");
        System.out.println(queue.toString());
        queue.add(items.get(6));
        System.out.println("7, 6");
        System.out.println(queue.toString());
        queue.add(items.get(7));
        System.out.println("5, 4");
        System.out.println(queue.toString());
        boolean out = queue.tryInsertWithLaxityCheck(items.get(8), 0L);
        System.out.println("6, 3");
        System.out.println("out: " + out + " queue: " + queue.toString());

        testLaxityObject obj = new testLaxityObject(new PriorityObject(8L, 6L));
        out = queue.tryInsertWithLaxityCheck(obj, 0L);
        System.out.println(obj);
        System.out.println("out: " + out + " queue: " + queue.toString());
        out = queue.laxityCheck(obj, 0L);

        obj = new testLaxityObject(new PriorityObject(9L, 7L));
        out = queue.laxityCheck(obj, 0L);
        System.out.println(obj);
        System.out.println("laxityCheck out: " + out + " queue: " + queue.toString());

        obj = new testLaxityObject(new PriorityObject(9L, 8L));
        out = queue.laxityCheck(obj, 0L);
        System.out.println(obj);
        System.out.println("laxityCheck out: " + out + " queue: " + queue.toString());

        System.out.println("====================");

    }

    @Test
    public void TestInsert(){
        testInsertQueue(queueMin);
        testInsertQueue(queueDefault);
    }


    @Test
    public void TestInsertCheck(){
        testInsertCheckQueue(queueMin);
        testInsertCheckQueue(queueDefault);
    }

    @Test
    public void Measurement(){
        //measurementQueue(queueMin);
        measurementQueue(queueDefault);
    }

    public void measurementQueue(MinLaxityWorkQueue<testLaxityObject> queue){
        int numTimestamps = 10000;
        Long[] timestamps = new Long[numTimestamps];
        int windowSize = 10;
        int entriesPerTs = 10;
        int costRange  = 10;
        int extension = 1000;
        int baseQueueSize = 10000;

        for(int i =0 ; i < numTimestamps; i++ ){
            timestamps[i] = (long)i;
        }
        testLaxityObject[] objectsBuffer = new testLaxityObject[numTimestamps * entriesPerTs];
        for(int i = 0; i < numTimestamps; i++){
            for(int j = 0; j < entriesPerTs; j++){
                Long ec = ThreadLocalRandom.current().nextLong(costRange);
                long laxity = timestamps[i] + extension;
                long priority = laxity + ec ;
                //System.out.println("i: " + i + " j: " + j + " index " + (i * numTimestamps + j) + " buffer size " + objectsBuffer.length);
                objectsBuffer[i * entriesPerTs + j] = new testLaxityObject(new PriorityObject(priority, laxity));
            }
        }
        //pre-insert
        int curInsertIndex = 0;
        for(int i = 0; i < baseQueueSize; i++){
            queue.add(objectsBuffer[curInsertIndex]);
            curInsertIndex ++;
        }

        Long start = System.currentTimeMillis();
        while(curInsertIndex < objectsBuffer.length){
            queue.add(objectsBuffer[curInsertIndex]);
            queue.poll();
            curInsertIndex++;
        }
        System.out.println("Insertion/Deletion total " + (System.currentTimeMillis() - start));

        start = System.currentTimeMillis();
        curInsertIndex = baseQueueSize;
        start = System.currentTimeMillis();
        while(curInsertIndex < objectsBuffer.length){
            boolean success = queue.tryInsertWithLaxityCheck(objectsBuffer[curInsertIndex]);
            if(success){
                queue.poll();
            }
            curInsertIndex++;
        }
        System.out.println("tryInsertWithLaxityCheck total " + (System.currentTimeMillis() - start));

//        start = System.currentTimeMillis();
//        curInsertIndex = baseQueueSize;
//        while(curInsertIndex < objectsBuffer.length){
//            if(ThreadLocalRandom.current().nextDouble() < 0.5){
//                queue.add(objectsBuffer[curInsertIndex]);
//                queue.poll();
//            }
//            else{
//                queue.laxityCheck(objectsBuffer[curInsertIndex]);
//            }
//            curInsertIndex++;
//        }
//        System.out.println("laxityCheck total " + (System.currentTimeMillis() - start));

        // Insert
//        testInsertQueue(queueMin);
//        testInsertQueue(queueDefault);
    }
//    class queueItem implements Comparable {
//        Integer deadline;
//        Integer laxity;
//        public queueItem(Integer deadline, Integer laxity){
//            this.deadline = deadline;
//            this.laxity = laxity;
//        }
//
//        @Override
//        public int compareTo(Object o) {
//            int compare = this.deadline.compareTo(((queueItem)o).deadline);
//            if (compare != 0) return compare;
//            return this.deadline.hashCode() - (((queueItem)o).deadline.hashCode());
//        }
//
//        @Override
//        public String toString(){
//            return String.format("<%s, %s>", this.deadline, this.laxity);
//        }
//    }

    class testLaxityObject extends LaxityComparableObject{
        private PriorityObject priority;

        public testLaxityObject(PriorityObject priorityObject){
            this.priority = priorityObject;
        }

        @Override
        public PriorityObject getPriority() throws Exception {
            return this.priority;
        }

        @Override
        public String toString(){
            return "testObject: " + priority.toString();
        }
    }
}
