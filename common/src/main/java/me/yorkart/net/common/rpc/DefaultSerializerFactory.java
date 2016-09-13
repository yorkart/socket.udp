package me.yorkart.net.common.rpc;

import org.apache.thrift.TBase;

/**
 * Factory that maps serialization strings to serializers. Used to get a
 * Serializer from config serializer description.
 *
 * @author jay
 *
 */
public class DefaultSerializerFactory implements SerializerFactory {

    private static final String JAVA_SERIALIZER_TYPE_NAME = "java-serialization";
    private static final String STRING_SERIALIZER_TYPE_NAME = "string";
    private static final String IDENTITY_SERIALIZER_TYPE_NAME = "identity";
    private static final String JSON_SERIALIZER_TYPE_NAME = "json";
    private static final String PROTO_BUF_TYPE_NAME = "protobuf";
    private static final String THRIFT_TYPE_NAME = "thrift";

    public Serializer<?> getSerializer(SerializerDefinition serializerDef) {
        String name = serializerDef.getName();
        if(name.equals(THRIFT_TYPE_NAME)) {
            return new ThriftSerializer<TBase>(serializerDef.getCurrentSchemaInfo());
        } else {
            throw new IllegalArgumentException("No known serializer type: "
                    + serializerDef.getName());
        }
    }
}