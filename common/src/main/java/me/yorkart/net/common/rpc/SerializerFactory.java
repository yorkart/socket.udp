package me.yorkart.net.common.rpc;

/**
 * A Serializer factory creates serializers from a serializer definition
 *
 * @author jay
 *
 */
public interface SerializerFactory {

    public abstract Serializer<?> getSerializer(SerializerDefinition serializerDef);

}