package jichufs;

import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.apache.commons.io.FileUtils;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import spring.SpringConfig;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static jichufs.FSMessages.QueryMessage;
/**
 * Entry point for all nodes, will be running both 
 */
@Slf4j
public class App {
    // Threads for failure detection
    private static Server server;
    private static PingWorker pingWorker;
    private static Introducer introducer;

    // Data Storage
    private static MembershipList membershipList;
    private static Election election;
    private static Member master;
    private static Master curMaster;

    // Threads for sdfs management
    private static QueryWorker queryWorker;
    private static Replicant replicant;

    /**
    * Create the SDFS temporary directory named "/tmp"
    */
    public static void createSDFSDir() throws IOException {
        File dir = new File("tmp");
        if(!dir.exists()) {
            dir.mkdir();
        } else {
            FileUtils.deleteDirectory(dir);
            dir.mkdir();
        }
    }

    /**
    * Loop through the /tmp directory and print all the files it contains
    */
    public static void printSDFSFiles() {
        File dir = new File("tmp");
        File[] sdfsFiles = dir.listFiles();
        if(sdfsFiles != null) {
            for(File child : sdfsFiles) {
                System.out.println(child);
            }
        } else {
            System.out.println("No files found. Directory is empty or may not exist.");
        }
    }

    public static void main(String[] args) throws Exception {
        final ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringConfig.class);

        server = ctx.getBean(Server.class);
        membershipList = ctx.getBean(MembershipList.class);
        master = ctx.getBean(Member.class);
        curMaster = ctx.getBean(Master.class);
        election = ctx.getBean(Election.class);
        pingWorker = ctx.getBean(PingWorker.class);
        replicant = ctx.getBean(Replicant.class);
        queryWorker = ctx.getBean(QueryWorker.class);

        var inGroup = false;

        final String introducerHostId = ctx.getEnvironment().getProperty("introducerHostId");
        final int port = Integer.valueOf(ctx.getEnvironment().getProperty("introducerPort"));

        System.out.println(introducerHostId + " " + port);

        final Thread serverThread = new Thread(server);
        final Thread pingThread = new Thread(pingWorker);
        final Thread electionThread = new Thread(election);
        final Thread replicantThread = new Thread(replicant);

        if (introducerHostId.equals(InetAddress.getLocalHost().getHostName())) {
            introducer = ctx.getBean(Introducer.class);
            final Thread introducerThread = new Thread(introducer);

            var hostId = Introducer.createId(InetAddress.getLocalHost().getHostAddress());
            membershipList.setHostId(hostId);
            membershipList.add(hostId);
            election.setCurrent(hostId);
            election.setHighestSeen(hostId);
            serverThread.start();
            pingThread.start();
            introducerThread.start();
            electionThread.start();
            replicantThread.start();
        }

        var message = "Please select an option from 1-10: \n" +
                "1- List the membership list\n" +
                "2 - List self's id\n" +
                "3 - Join the group\n" +
                "4 - Voluntarily leave the group (different from a failure)\n" +
                "5 - PUT (localFile, sdfsfilename)\n" + 
                "6 - GET (localFile, sdfsfilename)\n" +
                "7 - DELETE (sdfsfilename)\n" +
                "8 - STORE (list files in SDFS)\n" +
                "9 - LS (sdfsfilename)\n" +
                "10 - get-versions (sdfsfilename, numVersions)\n" +
                "11 - List current master's id";


        int option;

        try(BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
            while (true) {
                System.out.println(message);
                String userInput = reader.readLine();
                if (userInput == null)
                    break;
                option = Integer.parseInt(userInput);
                switch (option) {
                    case 1:
                        System.out.println(membershipList);
                        break;
                    case 2:
                        System.out.println("SystemId: " + membershipList.getHostId());
                        break;
                    case 3:
                        if (inGroup) {
                            break;
                        }
                        inGroup = true;
                        createSDFSDir();
                        Socket socket = new Socket(introducerHostId, port);
                        ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
                        IntroducerMembershipDetails details = (IntroducerMembershipDetails) inputStream.readObject();
                        log.debug("Joining group with id " + details.getId());
                        membershipList.setHostId(details.getId());
                        membershipList.addAll(details.getMembersList());
                        master.setId(details.getMaster().getId());
                        master.setLastAlive(details.getMaster().getLastAlive());
                        membershipList.setMaster(master);
                        // Handle if this node is the master
                        if(master.getId().equals(membershipList.getHostId())) {
                            curMaster.setHostId(master.getId());
                            membershipList.setCurMaster(curMaster);
                            final Thread masterThread = new Thread(curMaster);
                            masterThread.start();
                        }
                        log.debug("Master is " + master.getId());
                        election.setCurrent(details.getId());
                        election.setHighestSeen(details.getId());
                        pingThread.start();
                        serverThread.start();
                        electionThread.start();
                        replicantThread.start();
                        break;
                    case 4:
                        if (inGroup) {
                            server.setLeave(true);
                            pingWorker.setLeave(true);
                            inGroup = false;
                        }
                        serverThread.join();
                        pingThread.join();
                        election.getServerSocket().close();
                        electionThread.join();
                        System.exit(0);
                        break;
                    case 5:
                        // PUT - (localfile, sdfsname): upload localfile under sdfsname
                        // Get localfile and sdfsname
                        queryWorker.run(FSMessages.QueryMessage.OpType.PUT, reader);
                        break;
                    case 6:
                        // GET - (localfile, sdfsname): download sdfsname uunder localfile
                        queryWorker.run(FSMessages.QueryMessage.OpType.GET, reader);
                        break;
                    case 7:
                        // DELETE - (sdfsname): remove sdfsname from the sdfs
                        queryWorker.run(FSMessages.QueryMessage.OpType.DELETE, reader);
                        break;
                    case 8:
                        // STORE - list files in this hosts sdfs | DONE
                        printSDFSFiles();
                        break;
                    case 9:
                        // LS - (sdfsname): list all machinese currently storing sdfsname
                        queryWorker.run(FSMessages.QueryMessage.OpType.LS, reader);
                        break;
                    case 10:
                        // VERSION - (sdfsname, version, localfile): get last <version> sdfsname and store in localfile
                        queryWorker.run(FSMessages.QueryMessage.OpType.VERSION, reader);
                        break;
                    case 11:
                        System.out.println("Master is " + master.getId());
                        break;
                    default:
                        System.out.println("Invalid input");
                        break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(0);
    }
}
