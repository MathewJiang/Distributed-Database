package app_kvServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.log4j.Logger;

import shared.ConnectionUtil;
import shared.ConsistentHash;
import shared.InfraMetadata;
import shared.InfraMetadata.ServiceLocation;
import shared.MD5;
import shared.messages.CommMessage;
import shared.messages.CommMessageBuilder;
import shared.messages.KVMessage.StatusType;
import app_kvServer.storage.Disk;
import app_kvServer.storage.Storage;

import com.google.gson.JsonSyntaxException;

/**
 * Represents a connection end point for a particular client that is connected
 * to the server. This class is responsible for message reception and sending.
 * The class also implements the echo functionality. Thus whenever a message is
 * received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {

	private static Logger logger = Logger.getRootLogger();

	private boolean isOpen;

	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;
	private KVServer callingServer;
	private boolean clientConnectionDown = false;
	private boolean isMigrating = false;

	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * 
	 * @param clientSocket
	 *            the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket, KVServer callingServer) {
		this.clientSocket = clientSocket;
		this.callingServer = callingServer;
		this.isOpen = true;
	}

	/**
	 * Initializes and starts the client connection. Loops until the connection
	 * is closed or aborted by the client.
	 */
	public void run() {
		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
			ConnectionUtil conn = new ConnectionUtil();

			try {
				CommMessage latestMsg = conn.receiveCommMessage(input);
				if (latestMsg == null) {
					// FIXME: other scenarios that result in (latestMsg ==
					// null)
					throw new IOException();
				}

				if (callingServer.isSuspended()) {
					// Sending messages to inform client that the server has
					// been stopped
					// as a way to ignoring the request
					CommMessage responseMsg = new CommMessageBuilder()
							.setStatus(StatusType.SERVER_STOPPED).build();
					conn.sendCommMessage(output, responseMsg);
				} else {
					StatusType op = latestMsg.getStatus();
					String key = latestMsg.getKey();
					String value = latestMsg.getValue();

					CommMessage responseMsg = new CommMessage();

					// If request key is not in server range. Issue an
					// update message to requesting client.
					if (!callingServer.hasKey(key)) {
						responseMsg.setInfraMetadata(callingServer
								.getClusterMD());
						responseMsg
								.setStatus(StatusType.SERVER_NOT_RESPONSIBLE);
					} else {
						switch (op) {
						case PUT:
							try {
								KVServer.serverLock.lock();
								StatusType status = handlePUT(key, value);
								responseMsg.setKey(key);
								responseMsg.setValue(value);
								responseMsg.setStatus(status);
							} catch (IOException e) {
								responseMsg.setStatus(StatusType.PUT_ERROR);
							} finally {
								KVServer.serverLock.unlock();
							}
							break;

						case GET:
							try {
								KVServer.serverLock.lock();
								value = handleGET(key);

								responseMsg.setKey(key);
								responseMsg.setValue(value);
								responseMsg.setStatus(StatusType.GET_SUCCESS);
							} catch (Exception e) {
								value = null;
								responseMsg.setStatus(StatusType.GET_ERROR);
							} finally {
								KVServer.serverLock.unlock();
							}
							break;

						default:
							logger.error("[Error]ClientConnection.java/run(): Unknown type of message");
							throw new IOException();
						}
					}

					conn.sendCommMessage(output, responseMsg);
				}

				/*
				 * connection either terminated by the client or lost due to
				 * network problems
				 */
			} catch (JsonSyntaxException e) {
				logger.error("[ClientConnection]/run(): Error! Received message experiences error during deserilization!");
			} catch (IOException ioe) {
				logger.error("Error! Connection lost!");
				isOpen = false;
			}

		} catch (IOException ioe) {
			logger.error("Error! Connection could not be established!", ioe);

		} finally {

			try {
				if (clientSocket != null) {
					input.close();
					output.close();
					clientSocket.close();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
		}
	}

	/****************************************************************************
	 * willMigrate Will the current server do migration !Assume: only one server
	 * needs migration at a time
	 * 
	 * @return true if the callingServer will do migration false if the
	 *         callingServer will not do migration
	 * 
	 * @throws Exception
	 *             if failed to find range !!!!!FIXME: need to change to
	 *             "private" !!!!!FIXME: does not consider server collision case
	 *             e.g. newly added server MD5 value coincides with existing
	 *             server
	 * 
	 ****************************************************************************/
	public boolean willMigrate(InfraMetadata oldMetaData,
			InfraMetadata newMetaData) throws Exception {
		ArrayList<ServiceLocation> oldServiceLocations = (ArrayList) oldMetaData
				.getServerLocations();
		ArrayList<ServiceLocation> newServiceLocations = (ArrayList) newMetaData
				.getServerLocations();
		boolean isAddNode = false;

		if (oldServiceLocations.size() == newServiceLocations.size()) {
			return false;
		} else if (oldServiceLocations.size() < newServiceLocations.size()) {
			isAddNode = true;
		} else {
			isAddNode = false;
		}

		ServiceLocation newNode = getChangedNode(oldMetaData, newMetaData);
		if (newNode == null) {
			return false;
		}

		BigInteger newNodeMD5 = MD5.getMD5(new String(newNode.host + ":"
				+ newNode.port));

		// FIXME: SUPER SUPER SLOW!!!
		ConsistentHash oldHash = new ConsistentHash();
		oldHash.addNodesFromInfraMD(oldMetaData);

		ConsistentHash newHash = new ConsistentHash();
		newHash.addNodesFromInfraMD(newMetaData);

		BigInteger[] selfRange = null;
		try {
			// FIXME: getName incorrect!!!
			ServiceLocation sl = new ServiceLocation(callingServer.getName(),
					callingServer.getHostname(), callingServer.getPort());
			selfRange = oldHash.getHashRangeInteger(sl);
		} catch (Exception e) {
			logger.error("[ClientConnection/willMigrate]failed to get selfRange!");
			e.printStackTrace();
			throw new Exception();
		}

		if (isAddNode) {
			if (((selfRange[1].compareTo(newNodeMD5)) == 1)
					&& ((selfRange[0].compareTo(newNodeMD5)) == -1)) {
				return true;
			} else {
				return false;
			}
		} else {
			if (selfRange[0].compareTo(newNodeMD5.add(BigInteger.ONE)) == 0) {
				return true;
			} else {
				return false;
			}
		}
	}

	/****************************************************************************
	 * getChangedNode Get node that has been added/removed from the system
	 * 
	 * @return corresponding ServiceLocation null if no change
	 * 
	 *         !!!!!FIXME: need to change to "private"
	 ****************************************************************************/
	public ServiceLocation getChangedNode(InfraMetadata oldMetaData,
			InfraMetadata newMetaData) {
		ArrayList<ServiceLocation> oldServiceLocations = (ArrayList) oldMetaData
				.getServerLocations();
		ArrayList<ServiceLocation> newServiceLocations = (ArrayList) newMetaData
				.getServerLocations();
		ServiceLocation newNode = null;

		// determine whether adding or removing node
		if (oldServiceLocations.size() == newServiceLocations.size()) {
			return null;
		} else if (oldServiceLocations.size() < newServiceLocations.size()) {
			// detect a new node being added
			HashSet<String> oldServiceLocationsSet = new HashSet<String>();
			for (ServiceLocation sl : oldServiceLocations) {
				oldServiceLocationsSet.add(sl.serviceName);
			}

			for (ServiceLocation sl : newServiceLocations) {
				if (!oldServiceLocationsSet.contains(sl.serviceName)) {
					newNode = sl;
					break;
				}
			}
			logger.info("[ClienConnection.java/willMigrate]New: "
					+ newNode.serviceName + " has been added!");
			System.out.println("[debug]ClienConnection.java/willMigrate: New: "
					+ newNode.serviceName + " has been added!");
		} else {
			// detect a node being removed
			HashSet<String> newServiceLocationsSet = new HashSet<String>();

			for (ServiceLocation sl : newServiceLocations) {
				newServiceLocationsSet.add(sl.serviceName);
			}

			for (ServiceLocation sl : oldServiceLocations) {
				if (!newServiceLocationsSet.contains(sl.serviceName)) {
					newNode = sl;
					break;
				}
			}
			logger.info("[ClienConnection.java/willMigrate]Node: "
					+ newNode.serviceName + " has been removed!");
			System.out
					.println("[debug]ClienConnection.java/willMigrate: Node: "
							+ newNode.serviceName + " has been removed!");
		}

		return newNode;
	}

	/****************************************************************************
	 * migrationTo Migrate data to the other server !Assume: only one server
	 * needs migration at a time
	 * 
	 * @throws IOException
	 ****************************************************************************/
	private void migrationTo(InfraMetadata oldMetaData,
			InfraMetadata newMetaData) throws IOException {
		// flush all data to the disk first
		try {
			Storage.flush();
		} catch (IOException e) {
			throw new IOException();
		}

		// clear up the cache
		callingServer.clearCache();

		ServiceLocation serverInfo = callingServer.getServerInfo();

		// check which server needs migration

		// check how many <key, value> pairs need to be migrated to
	}

	/****************************************************************************
	 * migrationFrom Received the data migrated from the other server !Assume:
	 * server invoking this method must the newly added server
	 * 
	 ****************************************************************************/
	private void migrationFrom(InfraMetadata newMetaData,
			InfraMetadata oldMetaData) {
		// check how many <key, value> pairs need to be migrated from

		// receive the number of times

		// migration done
	}

	/****************************************************************************
	 * handlePUT store the <key, value> pairs in persistent disk
	 * 
	 * @param key
	 * @param value
	 * 
	 * @return status after PUT operation
	 ****************************************************************************/
	synchronized private StatusType handlePUT(String key, String value)
			throws IOException {
		/*
		 * if (!Disk.if_init()) { logger. warn(
		 * "[ClientConnection]handlePUT: DB not initalized during Server startup"
		 * ); Disk.init(); //FIXME: should raise a warning/error }
		 * 
		 * return Disk.putKV(key, value);
		 */
		if (!Disk.if_init()) {
			logger.warn("[ClientConnection]handlePUT: DB not initalized during Server startup");
			Disk.init(); // FIXME: should raise a warning/error
		}

		return Storage.putKV(key, value);
	}

	/****************************************************************************
	 * handleGET get the corresponding value associated with key
	 * 
	 * @param key
	 * 
	 * @return value if found null if not found
	 ****************************************************************************/
	synchronized private String handleGET(String key) throws Exception {
		if (!Disk.if_init()) {
			logger.warn("[ClientConnection]handleGET: DB not initalized during Server startup");
		}
		return Storage.getKV(key);
	}

	/* Testing purposes only */
	public static void main(String[] args) {
		ClientConnection conn = new ClientConnection(new Socket(),
				new KVServer(50006, 5, "LRU"));
		InfraMetadata newMetaData = null;
		InfraMetadata oldMetaData = null;
		try {
			newMetaData = InfraMetadata
					.fromConfigFile("/nfs/ug/homes-3/j/jiangz32/ECE419/ECE419-Distributed-Systems/basicStorageServer/newMetaData.properties");
		} catch (Exception e) {
			e.printStackTrace();
		}

		if (newMetaData == null) {
			System.out.println("newMetaData null!");
		}

		try {
			oldMetaData = InfraMetadata
					.fromConfigFile("/nfs/ug/homes-3/j/jiangz32/ECE419/ECE419-Distributed-Systems/basicStorageServer/oldMetaData.properties");
		} catch (Exception e) {
			e.printStackTrace();
		}

		if (oldMetaData == null) {
			System.out.println("oldMetaData null!");
		}

		ServiceLocation sl = conn.getChangedNode(oldMetaData, newMetaData);
		System.out.println(sl);

		ConsistentHash ch = new ConsistentHash();
		ch.addNodesFromInfraMD(oldMetaData);

		List<ServiceLocation> sls = oldMetaData.getServerLocations();
		for (ServiceLocation s : sls) {
			try {
				String[] hashrang = ch.getHashRange(s);
				System.out.println(s.serviceName + "'s Hashrange: "
						+ hashrang[0] + "~" + hashrang[1]);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		System.out
				.println("==================================================");
		ConsistentHash oldch = new ConsistentHash();
		ch.addNodesFromInfraMD(newMetaData);

		List<ServiceLocation> slss = newMetaData.getServerLocations();
		for (ServiceLocation s : slss) {
			try {
				String[] hashrang = ch.getHashRange(s);
				System.out.println(s.serviceName + "'s Hashrange: "
						+ hashrang[0] + "~" + hashrang[1]);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		System.out.println("server4's MD5 value: "
				+ MD5.getMD5String("127.0.0.1:50003"));

		try {
			if (conn.willMigrate(oldMetaData, newMetaData)) {
				System.out.println("willMigrate");
			} else {
				System.out.println("willNotMigrate");
			}
		} catch (Exception e) {
			System.exit(1);
			e.printStackTrace();
		}

	}

}
