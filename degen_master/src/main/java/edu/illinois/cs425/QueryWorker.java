package degenfs;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.context.annotation.Scope;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Random;
import java.net.Socket;
import java.net.InetAddress;

import static degenfs.FSMessages.QueryMessage;
import static degenfs.FSMessages.QueryResponseMessage;
/**
* Thread delegated by the main control flow to act as intermediate for user requests.
* Responsible for communicating between the master and using info to fulfill requests at Replicants.
*/
@Component()
@Getter
@Setter
@Slf4j
public class QueryWorker{
	
	// Process State Overhead
	@Autowired
	private MembershipList membershipList;
	@Autowired
	private Helper helper;
	@Autowired
	private Member master;

	// Networking Overhead
	private Socket socket;
	private DataInputStream in;
	private DataOutputStream out;
	@Value("${dataTransferPort}")
	int dataTransferPort;
	@Value("${masterPort}")
	int masterPort;

	public QueryWorker() {} // Put in to satisfy bean dependency

	/**
	* Write the contents of byte[] data into the local file "file"
	*/
	private void writeBytesToFile(byte[] data, String file) throws IOException {
		FileUtils.writeByteArrayToFile(new File(file), data);
	}

	/**
	* Read the contents of a file into a byte array and return it
	*/
	private byte[] getBytesFromFile(String file) throws IOException {
		return FileUtils.readFileToByteArray(new File(file));
	}

	private void shutConnection(Socket s, DataInputStream is, DataOutputStream os) {
		try {
			s.close();
			is.close();
			os.close();
		} catch(Exception e) {
			System.out.println("Failed shutting down connection in query..." + e);
		}
	}

	private void sendQuery(byte[] query) {
		try {
			out.writeInt(query.length);
			out.write(query);
		} catch(Exception e) {
			System.out.println("Failed to send query...");
			e.printStackTrace();
		}
	}

	private QueryResponseMessage getQueryResponse() {
		try {
			int length = in.readInt();
			byte[] response = new byte[length];
			in.readFully(response);
			return QueryResponseMessage.parseFrom(response);
		} catch(Exception e) {
			System.out.println("Failed reading query response from Master...");
			e.printStackTrace();
		}
		return null;
	}

	/**
	* Helper function to set up Master Socket for TCP communication
	*/
	private void setUpSocket(String id, int port) {
		try {
			this.socket = new Socket(InetAddress.getByName(id.split(":")[0]), port);
			this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
			this.out = new DataOutputStream(socket.getOutputStream());
		} catch(Exception e) {
			System.out.println("Failed setting up Master Socket for query..." + e);
		}
	}

	/** TODO: ADD CACHING CHECK
	* Contact Master to let it know we want to get a sdfs file sdfsName
	* Receive a list of replicas to get file from from Master.
	* Download the file from one of the replicas (random/heuristically) and store as localName
	*
	* @param sdfsName: String denoting the name the file exists under in the sdfs
	* @param localName: String denoting the name the file will exist under on our current host
	*/
	private void handleGet(String sdfsName, String localName) {
		log.debug("Sending GET for " + sdfsName);
		System.out.println("Contacting Master for <GET " + sdfsName + ">");
		setUpSocket(master.getId(), masterPort);
		var messageBuilder = FSMessages.QueryMessage.newBuilder()
			.setId(membershipList.getHostId())
			.setType(FSMessages.QueryMessage.OpType.GET)
			.setSdfsName(sdfsName);
		sendQuery(messageBuilder.build().toByteArray());

		// Handle the response
		QueryResponseMessage queryResponse = getQueryResponse();
		if(queryResponse.getStatus() == 200) {
			shutConnection(socket, in, out);
			// Choose a replica and contact it to retrieve a file
			ArrayList<String> replicaList = new ArrayList<String>(queryResponse.getReplicasList());
			String replicaId = replicaList.get(new Random().nextInt(replicaList.size()));
			setUpSocket(replicaId, dataTransferPort);
			try {
				var requestBuilder = FSMessages.RequestMessage.newBuilder()
					.setType(FSMessages.RequestMessage.ReqType.GET)
					.setSdfsName(sdfsName);
				sendQuery(requestBuilder.build().toByteArray());

				// Wait for an ACK (1) and then begin file download
				int ack = in.readInt();
				if(ack != 1) { System.out.println("Replica refused file..."); return;}
				int length = in.readInt();
				byte[] data = new byte[length];
				in.readFully(data);
				writeBytesToFile(data, localName);
				System.out.println("GOT " + sdfsName + ": Written to " + localName);
			} catch(Exception e) {
				System.out.println("Failed getting file from replica..." + e);
			}
		} else {
			System.out.println("File not available.");
		}
		shutConnection(socket, in, out);
	}

	/** DONE
	* Contact Master to let it know we want to put our local file localName
	* Receive a list of replicas to send file to from Master.
	* Send local file to each of the replicas under sdfsName. Send ack back to Master after done.
	*
	* @param sdfsName: string denoting the name the file will exist under in the sdfs
	* @param localName: string denoting the name the file exists under on our current host
	*/
	private void handlePut(String sdfsName, String localName) {
		log.debug("Sending PUT for " + sdfsName);
		System.out.println("Contacting Master for <PUT " + sdfsName + ">" + master.getId());
		setUpSocket(master.getId(), masterPort);
		System.out.println("wow.." + (out == null));
		var messageBuilder = FSMessages.QueryMessage.newBuilder()
			.setId(membershipList.getHostId())
			.setType(FSMessages.QueryMessage.OpType.PUT)
			.setSdfsName(sdfsName);
		sendQuery(messageBuilder.build().toByteArray());

		// Handle the reseponse
		QueryResponseMessage queryResponse = getQueryResponse();
		if(queryResponse.getStatus() == 200) {
			// Send file to all the replicas
			Socket tempSocket = null;
			DataInputStream tempIn = null;
			DataOutputStream tempOut = null;
			ArrayList<String> replicaList = new ArrayList<String>(queryResponse.getReplicasList());
			try {
				byte[] data = getBytesFromFile(localName);
				var requestBuilder = FSMessages.RequestMessage.newBuilder()
						.setType(FSMessages.RequestMessage.ReqType.PUT)
						.setSdfsName(sdfsName);
				byte[] query = requestBuilder.build().toByteArray();

				// Go through each replica and perform the send
				for(String replicaId : replicaList) {
					tempSocket = new Socket(InetAddress.getByName(replicaId.split(":")[0]), dataTransferPort);
					tempIn = new DataInputStream(new BufferedInputStream(tempSocket.getInputStream()));
					tempOut = new DataOutputStream(tempSocket.getOutputStream());
					tempOut.writeInt(query.length);
					tempOut.write(query);
					int ack = tempIn.readInt();
					// Wait for an ACK (1) and then send the file
					if(ack != 1) { System.out.println("Replica refused the file..."); return; }
					tempOut.writeInt(data.length);
					tempOut.write(data);
					shutConnection(tempSocket, tempIn, tempOut);
				}

				// Send ack to master
				out.writeInt(1);
				System.out.println("Uploaded " + localName + " as " + sdfsName);
			} catch(Exception e) {
				System.out.println("Failed to send file to replicas.." + e);
			}
		} else {
			System.out.println("Error grabbing replicas to send file to");
		}
		shutConnection(socket, in, out);
	}

	/** DONE
	* Send request to Master to delete all instances of sdfsName
	*/
	private void handleDelete(String sdfsName) {
		log.debug("Sending DELETE for" + sdfsName);
		// Send request to master
		System.out.println("Contacting Master for <DELETE " + sdfsName + ">");
		setUpSocket(master.getId(), masterPort);
		var messageBuilder = FSMessages.QueryMessage.newBuilder()
			.setId(membershipList.getHostId())
			.setType(FSMessages.QueryMessage.OpType.DELETE)
			.setSdfsName(sdfsName);
		sendQuery(messageBuilder.build().toByteArray());

		// Handle the response
		QueryResponseMessage queryResponse = getQueryResponse();
		if(queryResponse.getStatus() == 200) {
			Socket tempSocket = null;
			DataInputStream tempIn = null;
			DataOutputStream tempOut = null;
			ArrayList<String> replicaList = new ArrayList<String>(queryResponse.getReplicasList());
			try {
				var requestBuilder = FSMessages.RequestMessage.newBuilder()
						.setType(FSMessages.RequestMessage.ReqType.DELETE)
						.setSdfsName(sdfsName);
				byte[] query = requestBuilder.build().toByteArray();

				// Go through each replica and perform delete
				for(String replicaId : replicaList) {
					tempSocket = new Socket(InetAddress.getByName(replicaId.split(":")[0]), dataTransferPort);
					tempIn = new DataInputStream(new BufferedInputStream(tempSocket.getInputStream()));
					tempOut = new DataOutputStream(tempSocket.getOutputStream());
					tempOut.writeInt(query.length);
					tempOut.write(query);
					int ack = tempIn.readInt();
					if(ack != 1) { System.out.println("Replica refused delete."); return; }
					shutConnection(tempSocket, tempIn, tempOut);
				}
				out.writeInt(1);
				System.out.println("Succesfully Deleted File!");
			} catch(Exception e) {
				System.out.println("Error deleting files..." + e);
			}
		} 
		else {
			System.out.println("Succesfully Deleted File!");
		}
		shutConnection(socket, in, out);
	}

	/** DONE
	* Send request to Master to get the VM's that are storing sdfsName. Print these values to console. : DONE
	*/
	private void handleLS(String sdfsName) {
		log.debug("Sending LS for " + sdfsName);
		System.out.println("Contacting Master for <LS " + sdfsName + ">");
		// Connect to the Master and send request
		setUpSocket(master.getId(), masterPort);
		var messageBuilder = FSMessages.QueryMessage.newBuilder()
			.setId(membershipList.getHostId())
			.setType(FSMessages.QueryMessage.OpType.LS)
			.setSdfsName(sdfsName);

		sendQuery(messageBuilder.build().toByteArray());

		// Get Master response, handle, and close the connection
		QueryResponseMessage queryResponse = getQueryResponse();
		if(queryResponse.getStatus() == 200) {
			System.out.println(sdfsName + " stored at the following replicas: ");
			for(String replicaId : queryResponse.getReplicasList()) {
				System.out.println(replicaId);
			}
		}
		else {
			System.out.println(sdfsName + " not found in the file system.");
		}
		shutConnection(socket, in, out);
	}

	/**
	* Send request to Master to get the numVersions versions of sdfsname and store it into file localName
	*
	* @param sdfsName: String denoting the sdfs file we want to retrieve
	* @param localName: String denoting the local host file we will store results into
	* @param numVersions: int denoting the number of versions of the sdfs file we will retrieve
	*/
	private void handleVersion(String sdfsName, String localName, int numVersions) {
		log.debug("Sending VERSION for " + sdfsName + " and " + numVersions + " versions.");
		System.out.println("Contacting Master for <VERSION " + sdfsName + ">");
		// Connect to the Master and send request
		setUpSocket(master.getId(), masterPort);
		var messageBuilder = FSMessages.QueryMessage.newBuilder()
			.setId(membershipList.getHostId())
			.setType(FSMessages.QueryMessage.OpType.VERSION)
			.setSdfsName(sdfsName);
		sendQuery(messageBuilder.build().toByteArray());

		// Get Master response
		QueryResponseMessage queryResponse = getQueryResponse();
		if(queryResponse.getStatus() == 200) {
			shutConnection(socket, in, out);
			// Choose a replica and contact it to retrieve a file
			ArrayList<String> replicaList = new ArrayList<String>(queryResponse.getReplicasList());
			String replicaId = replicaList.get(new Random().nextInt(replicaList.size()));
			setUpSocket(replicaId, dataTransferPort);
			try {
				var requestBuilder = FSMessages.RequestMessage.newBuilder()
					.setType(FSMessages.RequestMessage.ReqType.GET)
					.setSdfsName(sdfsName)
					.setVersions(numVersions);
				sendQuery(requestBuilder.build().toByteArray());
				// Wait for an ACK (1) and then begin file download
				int ack = in.readInt();
				if(ack != 1) { System.out.println("Replica refused file..."); return;}
				int length = in.readInt();
				byte[] data = new byte[length];
				in.readFully(data);
				writeBytesToFile(data, localName);
				System.out.println("VERSIONED " + sdfsName + ": Written to " + localName);
			} catch(Exception e) {
				System.out.println("Failed sending file to replica..." + e);
			}
		} else {
			System.out.println("File not available.");
		}
		shutConnection(socket, in, out);
	}

	public void run(FSMessages.QueryMessage.OpType queryType, BufferedReader reader) {
		String sdfsName;
		String localName;
		//try(BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
		try{
			switch(queryType) {
				case GET:
					// Get sdfs file name and local filename then execute
					System.out.println("Input the sdfs filename you want to retrieve...");
					sdfsName = reader.readLine();
					System.out.println("Input the local filename you would like to store it as...");
					localName = reader.readLine();
					handleGet(sdfsName, localName);
					break;
				case PUT:
					// Get sdfs file name and local filename then execute
					System.out.println("Input the local filename you want to upload...");
					localName = reader.readLine();
					System.out.println("Input the sdfs filename you want to store the file as...");
					sdfsName = reader.readLine();
					handlePut(sdfsName, localName);
					break;
				case DELETE:
					// Get sdfs file name then execute
					System.out.println("Input the sdfs filename you want to DELETE...");
					sdfsName = reader.readLine();
					handleDelete(sdfsName);
					break;
				case LS:
					// Get sdfs filename then execute
					System.out.println("Input the sdfs filename you want to LS...");
					sdfsName = reader.readLine();
					handleLS(sdfsName);
					break;
				case VERSION:
					// Get sdfs filename, local filename, and versions then execute
					System.out.println("Input the sdfs filename youu want to retrieve...");
					sdfsName = reader.readLine();
					System.out.println("Input the local filename you would like to store it as...");
					localName = reader.readLine();
					System.out.println("Input the number of versions you would like to retrieve...");
					int versions = Integer.parseInt(reader.readLine());
					handleVersion(sdfsName, localName, versions);
					break;
				default: throw new RuntimeException("Invalid Query Type");
			}
		} catch(Exception e) {
			System.out.println("Failed handling query type " + queryType + "...");
			e.printStackTrace();
		}
	}


}