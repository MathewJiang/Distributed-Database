package client;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;

import shared.ConnectionUtil;
import shared.ConsistentHash;
import shared.InfraMetadata;
import shared.InfraMetadata.ServiceLocation;
import shared.messages.CommMessage;
import shared.messages.CommMessageBuilder;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import app_kvECS.ECS;

public class KVStore extends Thread implements KVCommInterface {
	private Logger logger = Logger.getRootLogger();
	private boolean running;

	// Resolved and resets on every request.
	private Socket srvSocket;
	private ServiceLocation target;

	private ECS ecs;
	private final int PORT_ECS = 39678;

	private InfraMetadata metaData;
	private ConsistentHash clientHash;
	
	public KVStore() throws IOException, InterruptedException {
		// Retrieve metadata from ecs.
		ecs = new ECS();
		
		// USE THIS
		ecs.connect("localhost", PORT_ECS);
		metaData = ecs.getMD();
		
		// Load metadata into a hash ring.
		clientHash = new ConsistentHash();
		clientHash.addNodesFromInfraMD(metaData);
	}

	public boolean isRunning() {
		return running;
	}

	public void setRunning(boolean run) {
		running = run;
	}

	public void resetClusterHash(InfraMetadata md) {
		metaData = md;
		clientHash = new ConsistentHash();
		clientHash.addNodesFromInfraMD(metaData);
	}

	private void resolveKVServer(String key) throws UnknownHostException,
			IOException {
		target = clientHash.getServer(key);
		logger.info("Resolved key " + key + " -> server" + target);
	}

	// Connect to target host (initialized by calling resolveKVServer).
	@Override
	public void connect() throws UnknownHostException, IOException {
		if (target == null) {
			throw new UnknownHostException("Target host is null!");
		}
		setRunning(true);
		logger.info("Connectiong to target server " + target);
		srvSocket = new Socket(target.host, target.port);
		logger.info("Connection established");
	}

	// Disconnect from host and reset target location to null.
	@Override
	public void disconnect() {
		try {
			srvSocket.close();
			target = null;
		} catch (IOException e) {
			logger.error("Error closing server socket");
			e.printStackTrace();
		}
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		try {
			// Resolve KV server destination and try to establish a connection.
			resolveKVServer(key);
			connect();
			logger.info("Connection established!");

			CommMessage cm = new CommMessageBuilder()
					.setStatus(KVMessage.StatusType.PUT).setKey(key)
					.setValue(value).build();
			ConnectionUtil conn = new ConnectionUtil();
			conn.sendCommMessage(srvSocket.getOutputStream(), cm);

			CommMessage latestMsg = conn.receiveCommMessage(srvSocket
					.getInputStream());
			System.out.println("Response: " + latestMsg);
			return latestMsg;
		} catch (IOException ioe) {
			logger.error("Connection lost!");
		} finally {
			disconnect();
		}
		return null;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		try {
			// Resolve KV server destination and try to establish a connection.
			resolveKVServer(key);
			connect();
			logger.info("Connection established!");

			CommMessage cm = new CommMessage(StatusType.GET, key.toString(),
					null);
			ConnectionUtil conn = new ConnectionUtil();
			conn.sendCommMessage(srvSocket.getOutputStream(), cm);

			CommMessage latestMsg = conn.receiveCommMessage(srvSocket
					.getInputStream());
			System.out.println("Response: " + latestMsg);
			return latestMsg;
		} catch (IOException ioe) {
			logger.error("Connection lost!");
		} finally {
			disconnect();
		}
		return null;
	}
}
