package jichufs;


import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static jichufs.FSMessages.Message;
@Component
@Slf4j
@Setter
public class Server implements Runnable{
    private DatagramSocket socket;
    private boolean leave;
    @Autowired
    private MembershipList membershipList;
    @Autowired
    private Helper helper;
    @Value("${serverPort}")
    private int serverPort;

    public Server(@Value("${serverPort}") int serverPort) throws Exception{
        socket = new DatagramSocket(serverPort);
    }

    public void serve(){
        //send message to introducer to populate membership list.
        while(!leave){
            byte[] buffer = new byte[10000];
            DatagramPacket packet = new DatagramPacket(buffer,buffer.length);
            try {
                socket.receive(packet);
                //log.debug("Received packet from "+socket.getInetAddress().getHostName());
                Message message = Message.parseFrom(Arrays.copyOfRange(buffer,0,packet.getLength()));
                handleMessage(packet.getAddress(),message);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void handleMessage(InetAddress fromAddress, Message message){
        switch (message.getType()){
            case ACK:
                handleAck(message);
                break;
            case PING:
                handlePing(fromAddress,message);
                break;
            case DATA:
                handleData(message);
                break;
            default: throw new RuntimeException("Invalid Packet Type");
        }
    }

    public void handleAck(Message message){
        //find message in sent pings by id. mark alive and increment timestamp;
        log.debug("Received ack from "+message.getId());
        membershipList.incrementLastAlive(message.getId()+"");
        membershipList.update(message);
    }

    public void handleData(Message message){
        membershipList.update(message);
    }

    /**
     * Handles PING message received from another node. Responds with an ACK that contains recent updates.
     * @param pingMessage
     */
    public void handlePing(InetAddress fromAddress, Message pingMessage){
        log.debug("Received ping from "+pingMessage.getId());
        Message.Builder messageBuilder = Message.newBuilder()
                                                .setId(pingMessage.getId())
                                                .setType(Message.PacketType.ACK)
                                                .setData(false);
        messageBuilder = helper.addEventUpdates(messageBuilder);
        Message ackMessage = messageBuilder.build();
        membershipList.update(pingMessage);
        byte[] ackMessageBytes = ackMessage.toByteArray();
        DatagramPacket ackPacket = new DatagramPacket(ackMessageBytes,ackMessageBytes.length,fromAddress,serverPort);
        try {
            socket.send(ackPacket);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setLeave(boolean leave){
        this.leave = leave;
    }
    @Override
    public void run() {
        this.serve();
    }
}

