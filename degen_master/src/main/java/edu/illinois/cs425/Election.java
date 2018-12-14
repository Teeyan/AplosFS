package degenfs;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import spring.SpringConfig;

import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.time.Instant;
import java.util.stream.Collectors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.io.*;

import java.util.*;
import java.net.*;
import degenfs.FSMessages.*;

@Component
@Getter
@Setter
@Slf4j

public class Election implements Runnable {

    // Use TCP for the election protocol
	private final ServerSocket serverSocket;
    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;
	@Autowired
    private MembershipList membershipList;
    @Value("${electionPort}")
    private int port;

    @Autowired
    private Member master;
    @Autowired
    private Master curMaster;
    
    private String current;
    private String highestSeen;
    private boolean hasSent = false;
    private AtomicBoolean electionRunning;

    public Election(@Value("${electionPort}") int port) throws Exception {

            // Communications between Master and Replicas uses TCP. 
            this.serverSocket = new ServerSocket(port);
            this.current = "";
            this.highestSeen = "";
            this.electionRunning = new AtomicBoolean(false);
            log.debug("Creating ElectionThread on port: " + Integer.toString(port));

    }

    private void sendMessage(byte[] message) {
        boolean sent = false;
        while(!sent) {
            // Get the neighbor of the current node.
            Member neighbor = membershipList.getNextNEntries(1).get(0);
            
            try {
                socket = new Socket(InetAddress.getByName(neighbor.getId().split(":")[0]), port);
                in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                out = new DataOutputStream(socket.getOutputStream());
                out.writeInt(message.length);
                out.write(message);
                sent = true;
            } catch (Exception e) {
                System.out.println("Connction refused " + e);
            }
        }

    }


    public void handle(ElectionMessage message) {

        // Ignore any messages whose initiator is less than the one that has already been seen.
        if (electionRunning.get()) {
            highestSeen = message.getInitiator();
            // Set the current master and forwards it 
            if (message.getType() == ElectionMessage.ElecType.ELECTED) {
                if(!message.getId().equals(current)) {
                    master.setId(membershipList.getMembersMap().get(message.getId()).getId());
                    master.setLastAlive(membershipList.getMembersMap().get(message.getId()).getLastAlive());
                    membershipList.setMaster(master);
                    electionRunning.set(false);
                    System.out.println("Elected new master: " + master.getId());
                    log.debug("Elected new master: " + master.getId());
                    sendMessage(message.toByteArray());
                    shutdownConnection();
                }
            }


            // If equal, then we know we are the new master.
            else if (message.getId().equals(current)) {
                master.setId(membershipList.getMembersMap().get(current).getId());
                master.setLastAlive(membershipList.getMembersMap().get(current).getLastAlive());
                membershipList.setMaster(master);
                electionRunning.set(false);
                sendMessage(ElectionMessage.newBuilder().setType(ElectionMessage.ElecType.ELECTED)
                                                        .setInitiator(current)
                                                        .setId(current).build().toByteArray());
                System.out.println("We are the new master! " + current);
                log.debug("We are ethe new master! " + current);
                curMaster.setHostId(membershipList.getHostId());
                membershipList.setCurMaster(curMaster);
                Thread masterThread = new Thread(curMaster);
                masterThread.start();
                shutdownConnection();
            }

            // If less than, then we update it w/ own value and send out.
            else if (message.getId().compareTo(current) < 0 && !hasSent) {
                hasSent = true;
                sendMessage(ElectionMessage.newBuilder().setType(message.getType())
                                           .setInitiator(message.getInitiator())
                                           .setId(current).build().toByteArray());
                shutdownConnection();
            }

            // If greater than, we just forward the packet.
            else {
                sendMessage(message.toByteArray());
                shutdownConnection();
            }
        }

    }

    public void initiate() {
        if (!electionRunning.get()) {
            electionRunning.set(true);
            sendMessage(ElectionMessage.newBuilder().setType(ElectionMessage.ElecType.ELECTION)
                                   .setInitiator(current)
                                   .setId(current).build().toByteArray());
            System.out.println("Starting election...");
        } 
    }


    public void shutdownConnection() {
        try {

        } catch(Exception e) {
            System.out.println("Shutting down connection in election failed..." + e);
            e.printStackTrace();
        }
    }

     /*
     * When the Election Thread is initialized, open a port and listen 
     * for new requests.
     */

    @Override
    public void run() {
        log.debug("ElectionThread is listening for events.");

        while (true) {
           
            byte[] buffer;
            int length;
            try {
                socket = serverSocket.accept();
                in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                out = new DataOutputStream(socket.getOutputStream());

                //TODO: Indicate that we are in election mode.
                length = in.readInt();
                buffer = new byte[length];
                in.readFully(buffer);
                ElectionMessage m = ElectionMessage.parseFrom(buffer);
                electionRunning.set(true);
                shutdownConnection();
                this.handle(m);
            } catch (SocketException s) {
                log.debug("Sucessfully exited the Election thread.");
                return;
            } catch (final Exception e) {
                e.printStackTrace();
            }
        }
    }	
}