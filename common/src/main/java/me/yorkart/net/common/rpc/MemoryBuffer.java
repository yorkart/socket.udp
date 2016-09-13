package me.yorkart.net.common.rpc;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.ByteArrayOutputStream;

/**
 * Created by Yorkart on 16/9/8.
 */
public class MemoryBuffer extends TTransport {

    protected ByteArrayOutputStream buffer;
    private int curPos;

    public MemoryBuffer() {
        this.buffer = new ByteArrayOutputStream();
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public void open() throws TTransportException {
        // do nothing
    }

    @Override
    public int read(byte[] buf, int off, int len) throws TTransportException {
        byte[] src = buffer.toByteArray();
        int amtToRead = (len > src.length - curPos ? src.length - curPos : len);
        if(amtToRead > 0) {
            System.arraycopy(src, curPos, buf, off, amtToRead);
            curPos += amtToRead;
        }
        return amtToRead;
    }

    @Override
    public void write(byte[] buf, int off, int len) throws TTransportException {
        buffer.write(buf, off, len);
    }

    public byte[] toByteArray() {
        return buffer.toByteArray();
    }
}
