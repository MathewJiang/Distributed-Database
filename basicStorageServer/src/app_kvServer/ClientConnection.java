package app_kvServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.Logger;

import shared.ConnectionUtil;
import shared.ConsistentHash;
import shared.messages.CommMessage;
import shared.messages.CommMessageBuilder;
import shared.messages.KVMessage.StatusType;
import shared.metadata.InfraMetadata;
import shared.metadata.ServiceLocation;
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

	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;
	private KVServer callingServer;
	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * 
	 * @param clientSocket
	 *            the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket, KVServer callingServer) {
		this.clientSocket = clientSocket;
		this.callingServer = callingServer;
	}

	private boolean shouldInformServerStopped(CommMessage msg) {
		return callingServer.isSuspended() && !msg.getFromServer();
	}

	private boolean shouldInformServerWriteLock(CommMessage msg) {
		return callingServer.isWriteLock() && !msg.getFromServer()
				&& msg.getStatus() == StatusType.PUT;
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

				if (shouldInformServerStopped(latestMsg)) {
					// Sending messages to inform client that the server has
					// been stopped as a way to ignoring the request.
					CommMessage responseMsg = new CommMessageBuilder()
							.setStatus(StatusType.SERVER_STOPPED).build();
					responseMsg.setFromServer(true);
					conn.sendCommMessage(output, responseMsg);
				} else if (shouldInformServerWriteLock(latestMsg)) {
					// Server is write-locked. The message is a put request from
					// client, reject. If the message is a get request or a
					// server put request, still accepts the operation.
					CommMessage responseMsg = new CommMessageBuilder()
							.setStatus(StatusType.SERVER_WRITE_LOCK).build();
					responseMsg.setFromServer(true);
					conn.sendCommMessage(output, responseMsg);
				} else {
					StatusType op = latestMsg.getStatus();
					String key = latestMsg.getKey();
					String value = latestMsg.getValue();

					CommMessage responseMsg = new CommMessage();

					// If request key is not in server range. Issue an
					// update message to requesting client.
					
					if (!callingServer.hasKey(key) && !callingServer.hasReplicaKey(key)) {
						logger.error("[run/ClientConnection.java]KEY NOT IN RANGE");
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
								
								
								// only send replica if current server is the coordinator
								// if (callingServer.hasKey(key)) {
								//	performReplication(latestMsg, 
								//			callingServer.getClusterMD(), callingServer.getServerInfo());
								// }
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
					responseMsg.setFromServer(true);
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
	
	
	/***********************************************************************
	 * performReplication
	 * do the replication mechanism
	 * after data has been modified by user 
	 * 
	 * @param	conn	connectionUtil
	 * @param	output	outputStream
	 * 
	 ***********************************************************************/
	private void performReplication(CommMessage latestMsg,
			InfraMetadata clusterMD, ServiceLocation serverInfo) {
		ConnectionUtil conn1 = new ConnectionUtil();
		ConnectionUtil conn2 = new ConnectionUtil();
		
		ConsistentHash ch = new ConsistentHash();
		ch.addNodesFromInfraMD(clusterMD);
		CommMessage replicaMessage = new CommMessageBuilder().setStatus(latestMsg.getStatus())
				.setKey(latestMsg.getKey()).setValue(latestMsg.getValue()).build();
		replicaMessage.setFromServer(true);
		replicaMessage.setIsReplicaMessage(true);
				
		
		try {
			// find immediate successor and send the message
			ServiceLocation successorFirst = ch.getSuccessor(serverInfo);
			logger.info("[replication/ClientConnection.java] successorFirstResponse: " + successorFirst.serviceName);
			Socket socketFirst = new Socket(successorFirst.host, successorFirst.port);
			conn1.sendCommMessage(socketFirst.getOutputStream(), replicaMessage);
			CommMessage successorFirstResponse = conn1.receiveCommMessage(socketFirst
					.getInputStream());
			
			if (successorFirstResponse.getStatus() != StatusType.PUT_SUCCESS) {
				logger.error("[replication/ClientConnection.java]" +
						"Error migrating message " + replicaMessage
						+ " from server " + serverInfo.serviceName + " to server "
						+ successorFirst.serviceName + "\nResponse: "
						+ successorFirstResponse);
			}
			socketFirst.close();
			
			
			// find successor of the immediate successor
			ServiceLocation successorSecond = ch.getSuccessor(successorFirst);
			logger.info("[replication/ClientConnection.java] successorSecondResponse: " + successorSecond.serviceName);
			Socket socketSecond = new Socket(successorSecond.host, successorSecond.port);
			conn2.sendCommMessage(socketSecond.getOutputStream(), replicaMessage);
			
			CommMessage successorSecondResponse = conn2.receiveCommMessage(socketSecond
					.getInputStream());
			if (successorSecondResponse.getStatus() != StatusType.PUT_SUCCESS) {
				logger.error("[replication/ClientConnection.java]" +
						"Error migrating message " + replicaMessage
						+ " from server " + serverInfo.serviceName + " to server "
						+ successorSecond.serviceName + "\nResponse: "
						+ successorSecondResponse);
			}
			socketSecond.close();
			
		} catch (Exception e) {
			logger.info(e.toString());
		}		
	}

}
