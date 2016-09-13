package me.yorkart.net.socket.udp.client.rpc;

/**
 * A Serializer factory creates serializers from a serializer definition
 *
 * @author jay
 *
 */
public interface SerializerFactory {

    public abstract Serializer<?> getSerializer(SerializerDefinition serializerDef);

}