package me.yorkart.net.common.rpc;

import com.sun.xml.internal.ws.encoding.soap.SerializationException;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * Created by Yorkart on 16/9/8.
 * @link http://www.programcreek.com/java-api-examples/index.php?source_dir=amor-master/Test/de.modelrepository.test/res/in/T0004/voldemort/src/java/voldemort/serialization/thrift/ThriftSerializer.java
 */
public class ThriftSerializer<T extends TBase> implements Serializer<T> {

    private static final String ONLY_JAVA_CLIENTS_SUPPORTED = "Only Java clients are supported currently, so the format of the schema-info should be: <schema-info>java=com.xyz.Foo,protocol=binary</schema-info> where com.xyz.Foo is the fully qualified name of the message.";

    /**
     * Supported Thrift Protocols
     */
    static enum ThriftProtocol {
        BINARY,
        JSON,
        SIMPLE_JSON,
        UNKNOWN
    }

    private Class<T> messageClass;
    private ThriftProtocol protocol;

    public ThriftSerializer(Class<T> clazz) {
        this.messageClass = clazz;
        this.protocol = getThriftProtocol("binary");
    }

    @SuppressWarnings("unchecked")
    public ThriftSerializer(String schemaInfo) {
        String[] thriftInfo = parseSchemaInfo(schemaInfo);

        if(thriftInfo[1] == null || thriftInfo[1].length() == 0) {
            throw new IllegalArgumentException("Thrift protocol is missing from schema-info.");
        }
        this.protocol = getThriftProtocol(thriftInfo[1]);
        if(this.protocol == ThriftProtocol.UNKNOWN) {
            throw new IllegalArgumentException("Unknown Thrift protocol found in schema-info");
        }

        if(thriftInfo[0] == null || thriftInfo[0].length() == 0) {
            throw new IllegalArgumentException("Thrift generated class name is missing from schema-info.");
        }
        try {
            this.messageClass = (Class<T>) Class.forName(thriftInfo[0]);
            Object msgObj = messageClass.newInstance();
            if(!(msgObj instanceof TBase)) {
                throw new IllegalArgumentException(thriftInfo[0]
                        + " is not a subtype of com.facebook.thrift.TBase");
            }
        } catch(ClassNotFoundException e) {
            throw new IllegalArgumentException(e);
        } catch(SecurityException e) {
            throw new IllegalArgumentException(e);
        } catch(InstantiationException e) {
            throw new IllegalArgumentException(e);
        } catch(IllegalAccessException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public byte[] toBytes(T object) {
        MemoryBuffer buffer = new MemoryBuffer();
        TProtocol protocol = createThriftProtocol(buffer);
        try {
            object.write(protocol);
        } catch(TException e) {
            throw new SerializationException(e);
        }
        return buffer.toByteArray();
    }

    public T toObject(byte[] bytes) {
        MemoryBuffer buffer = new MemoryBuffer();
        try {
            buffer.write(bytes);
        } catch(TTransportException e) {
            throw new SerializationException(e);
        }
        TProtocol protocol = createThriftProtocol(buffer);

        T msg = null;
        try {
            msg = messageClass.newInstance();
            msg.read(protocol);
        } catch(InstantiationException e) {
            throw new SerializationException(e);
        } catch(IllegalAccessException e) {
            throw new SerializationException(e);
        } catch(TException e) {
            throw new SerializationException(e);
        }

        return msg;
    }

    protected String[] parseSchemaInfo(String schemaInfo) {
        String[] thriftInfo = new String[2];

        String javaToken = null;
        String[] tokens = schemaInfo.split(";");
        for(int i = 0; i < tokens.length; i++) {
            if(tokens[i].trim().startsWith("java")) {
                javaToken = tokens[i];
                break;
            }
        }

        if(javaToken == null) {
            throw new IllegalArgumentException(ONLY_JAVA_CLIENTS_SUPPORTED);
        }

        tokens = javaToken.split(",");
        for(int i = 0; i < tokens.length; i++) {
            tokens[i] = tokens[i].trim();
            if(tokens[i].startsWith("java=")) {
                thriftInfo[0] = tokens[i].substring("java=".length());
            } else if(tokens[i].startsWith("protocol=")) {
                thriftInfo[1] = tokens[i].substring("protocol=".length());
            }
        }

        return thriftInfo;
    }

    protected ThriftProtocol getThriftProtocol(String protocolStr) {
        if(protocolStr.equalsIgnoreCase("binary")) {
            return ThriftProtocol.BINARY;
        } else if(protocolStr.equalsIgnoreCase("json")) {
            return ThriftProtocol.JSON;
        } else if(protocolStr.equalsIgnoreCase("simple-json")) {
            return ThriftProtocol.SIMPLE_JSON;
        } else {
            return ThriftProtocol.UNKNOWN;
        }
    }

    protected TProtocol createThriftProtocol(TTransport transport) {
        switch(this.protocol) {
            case BINARY:
                return new TBinaryProtocol(transport);
            case JSON:
                return new TJSONProtocol(transport);
            case SIMPLE_JSON:
                return new TSimpleJSONProtocol(transport);
            default:
                throw new IllegalArgumentException("Unknown Thrift Protocol.");
        }
    }
}