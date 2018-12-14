package degenfs;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.time.Instant;
import java.util.stream.Collectors;
import java.io.*;

import java.util.*;
import java.net.*;
import degenfs.FSMessages.*;


@Component
@Getter
@Setter
@Slf4j
public class Master implements Runnable {

    public static final int NOT_FOUND = 404;
    public static final int ERROR = 403;
    public static final int OK = 200;

	private final ServerSocket masterSocket;
	@Autowired
    private MembershipList membershipList;
    @Value("${masterPort}")
    private int port;
    @Value("${dataTransferPort}")
    private int dataPort;

    private String hostId;
    private Socket connectionSocket;
    private InputStream inFromClient;
    private OutputStream outToClient;


    // Keep a doubly-linked list, sdfsfile -> memberlist
    // and member -> sdfsFiles     
    private Map<String, MembershipList> fileToNodesMap = new HashMap<String, MembershipList>();
    private Map<Member, List<String>> nodeToFilesMap = new HashMap<Member, List<String>>();

    public Master(@Value("${masterPort}") int port) throws Exception {

        // Communications between Master and Replicas uses TCP. 
        this.masterSocket = new ServerSocket(port);
        this.hostId = "";

        log.debug("Creating Master on port: " + Integer.toString(port));

    }

    private void sendMessage(byte[] message, OutputStream o) {
        try {
            DataOutputStream out = new DataOutputStream(o);
            out.writeInt(message.length);
            out.write(message);
        } catch(Exception e) {
            System.out.println("Failed to send query..." + e);
        }
    }

    private QueryMessage getQueryMessage() {
        try {
            DataInputStream in = new DataInputStream(inFromClient);
            int length = in.readInt();
            byte[] response = new byte[length];
            in.readFully(response);
            return QueryMessage.parseFrom(response);
        } catch(Exception e) {
            System.out.println("Failed reading query response from Node..." + e);
        }
        return null;
    }

    public void rectifyNodeFailure(Member node) {

        // Remove node from meta-data tables.
        for (Map.Entry<String, MembershipList> entry : fileToNodesMap.entrySet()) {
            entry.getValue().getMembersMap().remove(node.getId());
        }

        nodeToFilesMap.remove(node);

        // Re-replicate the file out.
        reReplicateFiles();
    }

    private void reReplicateFiles() {
        for (Map.Entry<String, MembershipList> entry : fileToNodesMap.entrySet()) {

            Map<String, Member> membersMap = entry.getValue().getMembersMap();
            // Need to make sure that we don't include the Introducer Node in this process.
            if (membersMap.size() < 4 && membershipList.getMembersMap().size() >= 4) {

                int numberOfNewNodes = 4 - membersMap.size();
                List<Member> members = membershipList.getAllEntries();
                Collections.shuffle(members);

                List<Member> replicas = new ArrayList<Member>();

                int curIdx = 0;
                while (numberOfNewNodes > 0) {
                    if (!membersMap.containsKey(members.get(curIdx).getId())) {
                        replicas.add(members.get(curIdx));
                        numberOfNewNodes--;
                    }
                    curIdx++;
                }

                List<String> result = new ArrayList<String>();
                for (Member m : replicas) {
                    result.add(m.toString());
                }
                
                // Have the last replica in the list be responsible for re-replicating.
                Member[] candidates = membersMap.values().toArray(new Member[membersMap.size()]);
                Member temp = candidates[(new Random()).nextInt(candidates.length)];
                String ip = temp.getId().split(":")[0];
                
                byte[] request = RequestMessage.newBuilder().setType(RequestMessage.ReqType.REPLICATE)
                                               .setSdfsName(entry.getKey()).addAllSendTo(result).build().toByteArray();
                
                byte[] response = sendIndividualMessage(ip, request, false);
                
                for (Member m : replicas) {
                    updateMetaData(m, new String[]{entry.getKey()});
                }

            }
        }

    }

    private void shutConnection(Socket s, InputStream is, OutputStream os) {
        try {
            s.close();
            is.close();
            os.close();
        } catch(Exception e) {
            System.out.println("Failed shutting down connection in query..." + e);
        }
    }

    private void updateMetaData(Member m, String[] files) {
        if (!nodeToFilesMap.containsKey(m)) {
            nodeToFilesMap.put(m, new ArrayList<String>());
        }

        for (String file : files) {
            if (!fileToNodesMap.containsKey(file)) {
                fileToNodesMap.put(file, new MembershipList());
            }

            fileToNodesMap.get(file).getMembersMap().putIfAbsent(m.getId(), m);
            if (!nodeToFilesMap.get(m).contains(file)) {
                nodeToFilesMap.get(m).add(file);
            }
        }
 
    }

    private byte[] sendIndividualMessage(String ip, byte[] request, boolean isAck) {
        try {

            Socket s = new Socket(ip, dataPort);
            DataOutputStream out = new DataOutputStream(s.getOutputStream());
            DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream()));

            out.writeInt(request.length);
            out.write(request);
        
            int length = in.readInt();

            if (!isAck) {
                s.close();
                out.close();
                in.close();
                return null;
            }

            byte[] response = new byte[length];
            in.readFully(response);

            s.close();
            out.close();
            in.close();

            return response;
        } catch (Exception e) {
            log.debug("There was an error in trying to sendIndividualMessage.");
        }

        return null;
    }

    /*
     * When the Master is initialized we re-construct the fileMap
     * from pinging each of the machines. 
     */ 
    private void populateFileMap()  {

        // Get all of the Members including ourselves.
        var curMembers = membershipList.getAllEntries();

        // Loop through each member and ask for all of the data.
        for (Member member : curMembers) {
            // Construct the message.

            // Establish TCP connection with node and update the membership list.
            String ip = member.getId().split(":")[0];
            byte[] request = RequestMessage.newBuilder().setType(RequestMessage.ReqType.INFO)
                                           .setSdfsName("").build().toByteArray();

            byte[] response = sendIndividualMessage(ip, request, true);

            // Update the membership list from the response.
            String[] members = new String(response).split(",");
            if(!members[0].equals("")) {
                updateMetaData(member, members);
            }
        }

    }

    /*
     * This function is responsible for simply listing out the data that is
     * available for use.
     */
    private boolean checkMembershipList(String sdfsfile, OutputStream out)  {
        MembershipList members = fileToNodesMap.get(sdfsfile);
        
        if (members == null) {
            sendMessage(QueryResponseMessage.newBuilder().setStatus(NOT_FOUND).build().toByteArray(), out);
            return false;
        }
        
        List<String> sdfsNodes = new ArrayList<String>();
        members.getMembersMap().keySet().stream().forEach(key -> sdfsNodes.add(key));

        // Write the ArrayList out over the network.
        sendMessage(QueryResponseMessage.newBuilder().setStatus(OK).addAllReplicas(sdfsNodes).build().toByteArray(), out);

        return true;

    }

    private void handleDelete(String sdfsfile, OutputStream out, InputStream input) {
        if (checkMembershipList(sdfsfile, out)) {
            try {
                DataInputStream in = new DataInputStream(input);
                
                if (in.readInt() == 1) {
                    for (Member m : fileToNodesMap.get(sdfsfile).getAllEntries()) {
                        nodeToFilesMap.get(m).remove(sdfsfile);
                    }

                    fileToNodesMap.remove(sdfsfile);
                }
            } catch (IOException i) {
                // Do NOT remove the file from the query in this case.
                log.debug("Unexpected close on the Node processing the query.");
            }
        }

    }

    
    private void handlePut(String sdfsfile, OutputStream out, InputStream input) {
        List<Member> members;

        if (!fileToNodesMap.containsKey(sdfsfile)) {

            members = membershipList.getAllEntries();
            Collections.shuffle(members);

            int end_idx = Math.min(4, members.size());
            members = members.subList(0,end_idx);

        }
        else {
            members = fileToNodesMap.get(sdfsfile).getAllEntries();
        }

        List<String> list_ids = new ArrayList<String>();

        for (Member m : members) {
            list_ids.add(m.toString());
        }

        sendMessage(QueryResponseMessage.newBuilder().setStatus(OK).addAllReplicas(list_ids).build().toByteArray(), out);

        try {
            DataInputStream in = new DataInputStream(input);
            int val = in.readInt();

            if (val == 1) {
                for (Member m : members) {
                    updateMetaData(m, new String[]{sdfsfile});
                }
            }
        
        } catch (IOException e) {
            log.debug("Unexpected close on handling PUT.");
        }
        
    }


    /* Handle the input message that is received through the 
     * port and act accordingly.
     */
    public void handle(InputStream inStream, OutputStream outStream) throws IOException {
        QueryMessage message = getQueryMessage();
        String sdfsfile = message.getSdfsName();
        // TODO: Handle query message accordingly.
        switch (message.getType()) {
            case PUT:
                handlePut(sdfsfile, outStream, inStream);
                break;
            case GET:
                // As far as role of master, exact same logic.
                checkMembershipList(sdfsfile, outStream);
                break;
            case DELETE:
                handleDelete(sdfsfile, outStream, inStream);
                break;
            case LS:
                checkMembershipList(sdfsfile, outStream);
                break;
            case VERSION:
                checkMembershipList(sdfsfile, outStream);
                break;
            default: throw new RuntimeException("Invalid Packet Type given to Master!");
        }
            
    }

    /*
     * When the Master is initialized, open a port and listen 
     * for new requests.
     */

    @Override
    public void run() {
        log.debug("Master is listening for queries.");

        // Populate the Datastructures that it needs.
        populateFileMap();
        reReplicateFiles();

        while (true) {
            try {
                System.out.println("Waiting for call on port: " + port);
                
                // Declare buffers for reading in the data.
                connectionSocket = masterSocket.accept();
                inFromClient = connectionSocket.getInputStream();
                outToClient = connectionSocket.getOutputStream();

                // Handle the message.
                this.handle(inFromClient, outToClient);

                // When done, close the socket.
                shutConnection(connectionSocket, inFromClient, outToClient);
            } catch (final Exception e) {
                e.printStackTrace();
            }
        }
    }
    
}
