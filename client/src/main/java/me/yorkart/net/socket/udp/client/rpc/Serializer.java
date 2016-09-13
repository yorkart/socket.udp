package me.yorkart.net.socket.udp.client.rpc;

/**
 * Created by Yorkart on 16/9/8.
 */
public interface Serializer<T> {

    /**
     * Construct an array of bytes from the given object
     *
     * @param object The object
     * @return The bytes taken from the object
     */
    public byte[] toBytes(T object);

    /**
     * Create an object from an array of bytes
     *
     * @param bytes An array of bytes with the objects data
     * @return A java object serialzed from the bytes
     */
    public T toObject(byte[] bytes);

}