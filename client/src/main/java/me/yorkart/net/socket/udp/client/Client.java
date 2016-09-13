package me.yorkart.net.socket.udp.client;

import me.yorkart.net.common.rpc.dto.TSqlMetaData;
import me.yorkart.net.common.rpc.ThriftSerializer;

import java.io.IOException;
import java.net.*;

/**
 * Created by Yorkart on 16/9/2.
 */
public class Client {

    public static void main(String[] args) throws IOException {
        DatagramSocket clientSocket = new DatagramSocket();
//        InetAddress IPAddress = new InetAddress("localhost");
        SocketAddress address = new InetSocketAddress("127.0.0.1", 9877);

//        String sentence = "client something";
//        byte[] sendData = sentence.getBytes();
//        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, address);

        ThriftSerializer<TSqlMetaData> serializer = new ThriftSerializer<TSqlMetaData>("java=me.yorkart.net.common.rpc.dto.TSqlMetaData,protocol=binary");

        for (int i=0; i< 100; i++) {
            TSqlMetaData agentInfo = new TSqlMetaData("agentId",0L,1,"SQL");
            byte[] b = serializer.toBytes(agentInfo);
            for(byte bb : b) {
                System.out.print(bb);
                System.out.print(",");
            }
            DatagramPacket sendPacket0 = new DatagramPacket(b, b.length, address);
            clientSocket.send(sendPacket0);
        }

        System.out.println("send finish");
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        clientSocket.close();
    }

}
