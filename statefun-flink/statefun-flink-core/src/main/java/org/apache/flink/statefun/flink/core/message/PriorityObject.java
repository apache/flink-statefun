package org.apache.flink.statefun.flink.core.message;

import java.io.Serializable;
import java.util.Objects;

public class PriorityObject implements Comparable, Serializable {
  public Long priority;
  public Long nano;
  public Integer hash;
  public Long laxity;

  PriorityObject(Long rawPriority){
    this.priority = rawPriority;
    this.nano = System.nanoTime();
    this.hash = this.hashCode();
    this.laxity = rawPriority;
  }

  public PriorityObject(Long rawPriority, Long laxity){
    this.priority = rawPriority;
    this.nano = System.nanoTime();
    this.hash = this.hashCode();
    this.laxity = laxity;
  }

  @Override
  public int compareTo(Object o) {
    if(o == null){
      return 1;
    }
    int ret = this.priority.compareTo(Objects.requireNonNull((PriorityObject)o).priority);
    if(ret!=0) return ret;
    ret = this.nano.compareTo(Objects.requireNonNull((PriorityObject)o).nano);
    if(ret!=0) return ret;
    return hash - ((PriorityObject)o).hash;
  }

  @Override
  public String toString(){
    return String.format("%d:%d:%d %d", priority, nano, hash, laxity);
  }
}
