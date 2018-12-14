package degenfs;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.HashMap;
import java.util.ArrayList;
import java.lang.StringBuilder;
import java.net.Socket;
import java.net.ServerSocket;
import java.net.InetAddress;

import static degenfs.FSMessages.RequestMessage;
/**
* Always on thread responsible for responding to Query Thread requests from other servers.
* Handles the following:
* 	PUT: send ack to Query, receive the data, write it to disk, close the socket
*	GET: send data over, close the socket
*	DELETE: delete locally, send ack
*	REPLICATE: get file and list of replicas. send files to the replicas and ack back to master
*/
@Component
@Getter
@Setter
@Slf4j
public class Replicant implements Runnable {
	
	// Process State Overhead
	@Autowired
	private MembershipList membershipList;
	@Autowired
	private Helper helper;
	@Autowired
	private Member master;
	private boolean leave;

	// Networking Overhead
	private ServerSocket serverSocket;
	private Socket socket;
	private DataInputStream in;
	private DataOutputStream out;
	@Value("${dataTransferPort}")
	int dataTransferPort;
	@Value("${masterPort}")
	int masterPort;

	private byte[] request;	// handles storing the incoming request
	private int length; // length of the incoming request
	private RequestMessage requestMessage;
	private HashMap<String, Integer> fileVersionMap; // HashMap storing local sdfs filenames to their current version

	public Replicant(@Value("${dataTransferPort}") int dataPort) throws Exception{
		System.out.println("Setting up replicant to listen on port " + dataPort);
		this.serverSocket = new ServerSocket(dataPort, 10);
		this.fileVersionMap = new HashMap<String, Integer>();
	}

	private void shutConnection() {
		try {
			socket.close();
			in.close();
			out.close();
		} catch(Exception e) {
			System.out.println("Error in closing socket on replicant..." + e);
		}
	}

	/**
	* Download a file from QueryWorker and store it as sdfsName. Update metadata for it in our fileVersionMap
	* 	If it exists: increment version number and write it to <file>:<newversion>
	*	Else: put <filename, 1> into fileVersionMap and wriite efile to <file>:1
	*
	* @param sdfsName: String denoting the name the file should be stored under
	*/
	private void handlePut(String sdfsName) {	// DONE
		log.debug("Handling PUT on" + sdfsName);
		try {
			// Retrieve the file from the QueryWorker
			out.writeInt(1);
			int dataLen = in.readInt();
			byte[] data = new byte[dataLen];
			in.readFully(data);
			// Check if file exists under some version, otherwise write it to file as version 1
			if(fileVersionMap.containsKey(sdfsName)) {
				fileVersionMap.put(sdfsName, fileVersionMap.get(sdfsName) + 1);
			} 
			else {
				fileVersionMap.put(sdfsName, 1);
			}
			String path = "tmp/" + sdfsName + ":" + Integer.toString(fileVersionMap.get(sdfsName));
			FileUtils.writeByteArrayToFile(new File(path), data);
		} catch(Exception e) {
			System.out.println("Failed to write file to /tmp..." + e);
		}
	}

	/**
	* Send a file over to the QueryWorker. If numVersions is 0 then send the latest version.
	* Otherwise this is a VERSION request and we need to send a file containing max(numVersions, MAX_VERSION)
	*
	* @param sdfsName: String denoting name of the file to be sent
	* @param numVersions: int denoting the numbere of versions to send (0 if not a VERSION request)
	*/
	private void handleGet(String sdfsName, int numVersions) {
		log.debug("Handling GET on " + sdfsName);
		try {
			// GET Request
			if(numVersions == 0) {
				out.writeInt(1);
				String path = "tmp/" + sdfsName + ":" + Integer.toString(fileVersionMap.get(sdfsName));
				byte[] data = FileUtils.readFileToByteArray(new File(path));
				out.writeInt(data.length);
				out.write(data);
			}
			else {
				out.writeInt(1);
				StringBuilder builder = new StringBuilder();
				int latestVer = fileVersionMap.get(sdfsName);
				String file = "";
				String path = "tmp/" + sdfsName + ":";
				numVersions = numVersions < latestVer ? numVersions : latestVer;
				for(int i = numVersions; i > 0; i --) {
					// Read file into the String
					builder.append("v" + Integer.toString(i) + "\n");
					file = FileUtils.readFileToString(new File(path + Integer.toString(i)), "UTF-8");
					builder.append(file);
					builder.append("\n\n");
				}
				byte[] data = builder.toString().getBytes("UTF-8");
				out.writeInt(data.length);
				out.write(data);
			}
		} catch(Exception e) {
			System.out.println("Failed to send over " + sdfsName + "..." + e);
		}
	}

	/**
	* Delete file from our /tmp directory. If it doesnt exist, immediately send ack
	* Delete every file version by using fileVersionMap and purge entry from the map
	*
	* @param sdfsName: name of file to be deleted
	*/
	private void handleDelete(String sdfsName) { // DONE
		log.debug("Handling DELETE on " + sdfsName);
		try {
			// Delete the file, purge metadata, and send an ack back to the QueryWorker
			if(fileVersionMap.containsKey(sdfsName)) {
				int numVersions = fileVersionMap.get(sdfsName);
				String path = "tmp/" + sdfsName + ":";
				// Delete every version of the file
				for(int i = 0; i < numVersions; i++) {
					FileUtils.deleteQuietly(new File(path + Integer.toString(i + 1)));
				}
				fileVersionMap.remove(sdfsName);
			}
			out.writeInt(1);
		} catch(Exception e) {
			System.out.println("Failed to delete file from /tmp");
		}
	}

	/**
	* Send file sdfsName to all the replicas in replicaList, send ack back once complete
	* Send a file version by version. Note that the way handleGet is implemented, we do not need to specify version name
	*
	* @param sdfsName: String denoting the name of the file we want to send
	* @param replicaList: ArrayList<String> denoting nodes that will become replicas of the file
	*/
	private void handleReplicate(String sdfsName, ArrayList<String> replicaList) {	// DONE
		log.debug("Handling REPLICATE on " + sdfsName);
		try {
			Socket tempSocket = null;
			DataInputStream tempIn = null;
			DataOutputStream tempOut = null;
			var messageBuilder = FSMessages.RequestMessage.newBuilder()
				.setType(FSMessages.RequestMessage.ReqType.PUT)
				.setSdfsName(sdfsName);
			byte[] request = messageBuilder.build().toByteArray();
			int numVersions = fileVersionMap.get(sdfsName);
			byte[] data;
			String path = "tmp/" + sdfsName + ":";
			for(String replicaId : replicaList) {
				for(int i = 0; i < numVersions; i ++) {
					tempSocket = new Socket(InetAddress.getByName(replicaId.split(":")[0]), dataTransferPort);
					tempIn = new DataInputStream(new BufferedInputStream(tempSocket.getInputStream()));
					tempOut = new DataOutputStream(tempSocket.getOutputStream());
					tempOut.writeInt(request.length);
					tempOut.write(request);
					data = FileUtils.readFileToByteArray(new File(path + Integer.toString(i + 1)));
					int ack = tempIn.readInt();
					if(ack != 1){ System.out.println("Replication refusing file..."); return; }
					tempOut.writeInt(data.length);
					tempOut.write(data);
					tempSocket.close();
					tempIn.close();
					tempOut.close();
				}
			}
			out.writeInt(1);
		} catch(Exception e) {
			System.out.println("Failed to replicate file " + sdfsName + "..." + e);
			e.printStackTrace();
		}
	}

	/**
	* Send information about our /tmp directory to the Master.
	* Simply loop through our hashmap keys and send that information to the master.
	*/
	private void handleInfo() {
		log.debug("Sending Master INFO...");
		try {
			String fileSummary = "";
			for(String file : fileVersionMap.keySet()) {
				fileSummary += file + ",";
			}
			if(fileSummary.length() != 0) {
				fileSummary = fileSummary.substring(0, fileSummary.length() - 1);
			}
			byte[] data = fileSummary.getBytes("UTF-8");
			out.writeInt(data.length);
			out.write(data);
		} catch(Exception e) {
			System.out.println("Failed to send file info to master..." + e);
		}
	}

	@Override
	public void run() {
		try {
			while(!leave) {
				/* Listening on port for requests */
				socket = serverSocket.accept();
				in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
				out = new DataOutputStream(socket.getOutputStream());

				// Get the request type and pass over control
				length = in.readInt();
				request = new byte[length];
				in.readFully(request);
				requestMessage = RequestMessage.parseFrom(request);
				FSMessages.RequestMessage.ReqType requestType = requestMessage.getType();
				System.out.println("RECEIVED REQUEST FOR..." + requestType);
				switch(requestType) {
					case GET:
						handleGet(requestMessage.getSdfsName(), requestMessage.getVersions());
						break;
					case PUT:
						handlePut(requestMessage.getSdfsName());
						break;
					case DELETE:
						handleDelete(requestMessage.getSdfsName());
						break;
					case REPLICATE:
						// Grab the list of replicas to send to
						ArrayList<String> replicaList = new ArrayList<String>(requestMessage.getSendToList());
						handleReplicate(requestMessage.getSdfsName(), replicaList);
						break;
					case INFO:
						handleInfo();
						break;
					default: throw new RuntimeException("invalid request type to replicant");
				}
				shutConnection();
			}
			serverSocket.close();
		} catch(Exception e) {
			System.out.println("Error in persistent replicant thread..." + e);
		}
	}
}