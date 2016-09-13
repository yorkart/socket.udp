package me.yorkart.net.socket.udp.server;

import me.yorkart.net.common.rpc.ThriftSerializer;
import me.yorkart.net.common.rpc.dto.TSqlMetaData;

import java.io.IOException;
import java.net.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.Logger;

/**
 * Created by Yorkart on 16/9/2.
 */
public class Server {
    private static final Logger logger = Logger.getLogger(Server.class.getName());

    private DatagramSocket socket;
    private String bindAddress;
    private int port;

    private ThreadPoolExecutor io;

    public Server() {
        this.socket = createSocket(1024*1024 * 1024);
        this.bindAddress = "127.0.0.1";
        this.port = 89891;
        io = (ThreadPoolExecutor) Executors.newCachedThreadPool();
    }

    private DatagramSocket createSocket(int receiveBufferSize) {
        try {
            DatagramSocket socket = new DatagramSocket(null);
            socket.setReceiveBufferSize(receiveBufferSize);

                final int checkReceiveBufferSize = socket.getReceiveBufferSize();
                if (receiveBufferSize != checkReceiveBufferSize) {
                    logger.warning("DatagramSocket.setReceiveBufferSize() error. " + receiveBufferSize + "!=" + checkReceiveBufferSize);
                }

            socket.setSoTimeout(1000 * 5);
            return socket;
        } catch (SocketException ex) {
            throw new RuntimeException("Socket create Fail. Caused:" + ex.getMessage(), ex);
        }
    }

    private void bindSocket(DatagramSocket socket, String bindAddress, int port) {
        if (socket == null) {
            throw new NullPointerException("socket must not be null");
        }
        try {
            logger.info("DatagramSocket.bind() " + bindAddress + "/" + port);
            socket.bind(new InetSocketAddress(bindAddress, port));
        } catch (SocketException ex) {
            throw new IllegalStateException("Socket bind Fail. port:" + port + " Caused:" + ex.getMessage(), ex);
        }
    }

    public void start() {
        final DatagramSocket socket = this.socket;
        if (socket == null) {
            throw new IllegalStateException("socket is null.");
        }
        bindSocket(socket, bindAddress, port);

        for (int i = 0; i < 10; i++) {
            io.execute(new Runnable() {
                @Override
                public void run() {
                    receive(socket);
                }
            });
        }

    }

    private void receive(DatagramSocket socket) {
//        ThriftSerializer<TSqlMetaData> serializer = new ThriftSerializer<>("java=me.yorkart.net.common.rpc.dto.TSqlMetaData,protocol=binary");
        ThriftSerializer<TSqlMetaData> serializer = new ThriftSerializer<>(TSqlMetaData.class);

        try {
            byte[] receiveData = new byte[1024];
            while (true) {
                DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                socket.receive(receivePacket);

                TSqlMetaData tSqlMetaData = serializer.toObject(receivePacket.getData());
//                System.out.println(tSqlMetaData.toString());

//                String sentence = new String(receivePacket.getData());
//                System.out.println("RECEIVED: " + sentence);

                InetAddress ip = receivePacket.getAddress();
                int port = receivePacket.getPort();

                System.out.println("receive " + ip + ":" + port + " <= \n" + tSqlMetaData.toString());
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        InetSocketAddress address = new InetSocketAddress("0.0.0.0", 9877);
        DatagramSocket serverSocket = new DatagramSocket(address);
        serverSocket.setReceiveBufferSize(1024*1024);
        byte[] receiveData = new byte[1024];
        while(true)
        {
            System.out.println("try to receive");

            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
            serverSocket.receive(receivePacket);

            String sentence = new String(receivePacket.getData());
            System.out.println("RECEIVED: " + sentence);

            InetAddress ip = receivePacket.getAddress();
            int port = receivePacket.getPort();

            System.out.println("receive " + ip + ":" + port + " <= " + sentence);
        }
    }
}
