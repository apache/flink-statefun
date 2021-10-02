package org.apache.flink.statefun.flink.core.functions.utils;
import org.apache.flink.statefun.flink.core.message.PriorityObject;

import java.util.Objects;

public abstract class LaxityComparableObject implements Comparable {

    @Override
    public int compareTo(Object o) {
        try {
            return getPriority().compareTo(Objects.requireNonNull(((LaxityComparableObject)o).getPriority()));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    public abstract PriorityObject getPriority() throws Exception;

}
