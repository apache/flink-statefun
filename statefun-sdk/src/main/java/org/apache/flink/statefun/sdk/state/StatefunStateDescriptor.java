package org.apache.flink.statefun.sdk.state;
import org.apache.flink.api.common.typeutils.TypeSerializer;

//TODO
public interface StatefunStateDescriptor<T> {
    public TypeSerializer<T> getSerializer();
}
