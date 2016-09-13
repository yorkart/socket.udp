package me.yorkart.net.socket.udp.client.rpc;

import me.yorkart.net.socket.udp.client.rpc.dto.TSqlMetaData;

import java.util.Objects;

/**
 * Created by Yorkart on 16/9/8.
 */
public class ThriftSerializerTest {

    public static void main(String[] args) {

        new ThriftSerializerTest().testSerializerRoundtrip();
    }

    private void assertEquals(Object o1, Object o2) {
        System.out.println(
                Objects.equals(o1, o2) ? "equal" : "not equal"
        );
    }

    public void testGetSerializer() {
        SerializerDefinition def = new SerializerDefinition("thrift", "java="
                + TSqlMetaData.class.getName()
                + ", protocol=binary   ");
        Serializer<?> serializer = new DefaultSerializerFactory().getSerializer(def);
        assertEquals(ThriftSerializer.class, serializer.getClass());
    }

    public void testGetSerializer1() {
        SerializerDefinition def = new SerializerDefinition("thrift", "java="
                + TSqlMetaData.class.getName()
                + ",protocol=BiNary");
        Serializer<?> serializer = new DefaultSerializerFactory().getSerializer(def);
        assertEquals(ThriftSerializer.class, serializer.getClass());
    }

    public void testGetSerializer2() {
        try {
            SerializerDefinition def = new SerializerDefinition("thrift",
                    "java=" + TSqlMetaData.class.getName());
            new DefaultSerializerFactory().getSerializer(def);
        } catch(IllegalArgumentException e) {
            return;
        }
        System.out.println("IllegalArgumentException should have been thrown for missing Thrift protocol");
    }

    public void testGetSerializer3() {
        try {
            SerializerDefinition def = new SerializerDefinition("thrift", "protocol=json");
            new DefaultSerializerFactory().getSerializer(def);
        } catch(IllegalArgumentException e) {
            return;
        }
        System.out.println("IllegalArgumentException should have been thrown for missing Thrift class");
    }

    public void testGetSerializer4() {
        try {
            SerializerDefinition def = new SerializerDefinition("thrift", "");
            new DefaultSerializerFactory().getSerializer(def);
        } catch(IllegalArgumentException e) {
            return;
        }
        System.out.println("IllegalArgumentException should have been thrown for missing Thrift class and protocol");
    }

    public void testGetSerializer5() {
        try {
            SerializerDefinition def = new SerializerDefinition("thrift",
                    "java=com.abc.FooBar,protocol=simple-json");
            new DefaultSerializerFactory().getSerializer(def);
        } catch(IllegalArgumentException e) {
            return;
        }
        System.out.println("IllegalArgumentException should have been thrown for non-existing Thrift class");
    }

    public void testGetSerializer6() {
        try {
            SerializerDefinition def = new SerializerDefinition("thrift",
                    "java="
                            + TSqlMetaData.class.getName()
                            + ",protocol=bongus");
            new DefaultSerializerFactory().getSerializer(def);
        } catch(IllegalArgumentException e) {
            return;
        }
        System.out.println("IllegalArgumentException should have been thrown for bogus Thrift protocol");
    }

    public void testGetSerializer7() {
        try {
            SerializerDefinition def = new SerializerDefinition("thrift",
                    "php=FooBar,protocol=bongus");
            new DefaultSerializerFactory().getSerializer(def);
        } catch(IllegalArgumentException e) {
            return;
        }
        System.out.println("IllegalArgumentException should have been thrown for non-Java Thrift client");
    }

    public void testSerializerRoundtrip() {
        TSqlMetaData agentInfo = new TSqlMetaData("agentId",0L,1,"SQL");

        ThriftSerializer<TSqlMetaData> serializer = new ThriftSerializer<TSqlMetaData>("java=me.yorkart.net.socket.udp.client.rpc.dto.TSqlMetaData,protocol=binary");
        byte[] b = serializer.toBytes(agentInfo);
        TSqlMetaData message2 = serializer.toObject(b);

        assertEquals(agentInfo, message2);
    }

    public void testEmptyObjSeserialization() {
        TSqlMetaData message = new TSqlMetaData();
        ThriftSerializer<TSqlMetaData> serializer = new ThriftSerializer<TSqlMetaData>("java=me.yorkart.net.socket.udp.client.rpc.dto.TSqlMetaData, protocol=binary ");
        byte[] b = serializer.toBytes(message);
        TSqlMetaData message2 = serializer.toObject(b);

        assertEquals(message, message2);
    }
}
