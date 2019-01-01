package jichufs;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalUnit;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
@Component
@Setter
@Slf4j
public class PingWorker implements Runnable {

    @Autowired
    private MembershipList membershipList;
    @Autowired
    private Election election;
    @Autowired
    private Member master;
    @Autowired
    private Master curMaster;
    @Autowired
    private Helper helper;
    private DatagramSocket socket;
    private boolean leave;
    @Value("${serverPort}")
    private int serverPort;
    

    public PingWorker() throws  Exception{
        socket = new DatagramSocket();
    }

    public void run(){
        while(!leave) {
            var neighbors = membershipList.getNextNEntries(3);

            for (Member member : neighbors) {
                if (hasFailed(member)) {
                    membershipList.remove(member.getId());
                    // Check if failed member is the master. If it is then we need to start the election protocol
                    if(master.getId() == null || master.getId().equals(member.getId())) {
                        log.debug("Detected master failure. Starting election...");
                        election.initiate();
                    } else {
                        // If we are master we are responsible for rectifying a node failure
                        if(master.getId().equals(membershipList.getHostId())) {
                            log.debug("Rectifying node failure...");
                            curMaster.rectifyNodeFailure(member);
                        }
                    }
                }
            }

            //get new neighbors after detecting failures among its neighbors.
            neighbors = membershipList.getNextNEntries(3);

            if (membershipList.getMembersMap().size()>4)
                neighbors.add(membershipList.getRandomNeighbour(neighbors));

            membershipList.setLastPinged(neighbors);

            for (var neighbor : neighbors) {
                var messageBuilder = FSMessages.Message.newBuilder()
                        .setId(neighbor.getId())
                        .setData(false)
                        .setType(FSMessages.Message.PacketType.PING);
                messageBuilder = helper.addEventUpdates(messageBuilder);
                this.sendUDPMessage(messageBuilder, neighbor);
            }

            try {
                Thread.sleep(900);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        this.sendLeavePingToNeighbours();
    }

    private void sendLeavePingToNeighbours(){
        System.out.println("Sending ping message to neighbours:");
        membershipList.getLastPinged().forEach( member -> {
            System.out.println("Leave data send to: " + member.getId());
            var messageBuilder = FSMessages.Message.newBuilder()
                    .setId(member.getId())
                    .setData(true)
                    .setType(FSMessages.Message.PacketType.DATA);
            membershipList.getRecentUpdatesCache().put(membershipList.getHostId(),
                    FSMessages.Message.NodeEvent.newBuilder().setEvent(FSMessages.Message.Event.LEAVE).setId(membershipList.getHostId()).build());
            messageBuilder = helper.addEventUpdates(messageBuilder);
            this.sendUDPMessage(messageBuilder, member);
        });
    }

    private void sendUDPMessage(final FSMessages.Message.Builder messageBuilder, final Member neighbor){
        log.debug("Sending ping to " + neighbor.getId() + "which was last alive " + Duration.between(neighbor.getLastAlive(), Instant.now()).toMillis() + "ms ago");
        var messageBytes = messageBuilder.build().toByteArray();
        DatagramPacket packet = null;
        try {
            packet = new DatagramPacket(messageBytes,
                    messageBytes.length, InetAddress.getByName(neighbor.getId().split(":")[0]),
                    serverPort);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        try {
            socket.send(packet);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private boolean hasFailed(Member member){
        return (membershipList.getLastPinged()!= null && membershipList.getLastPinged().contains(member)) &&
                (Duration.between(member.getLastAlive(), Instant.now()).compareTo(Duration.ofSeconds(2)) > 0);

    }
}
