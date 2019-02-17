package app_kvServer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import shared.ConnectionUtil;
import shared.ConsistentHash;
import shared.InfraMetadata;
import shared.InfraMetadata.ServiceLocation;
import shared.messages.CommMessage;
import shared.messages.CommMessageBuilder;
import shared.messages.KVMessage.StatusType;
import app_kvECS.ECS;
import app_kvServer.storage.Disk;
import app_kvServer.storage.Storage;

public class KVServer extends Thread implements IKVServer {
	private static Logger logger = Logger.getRootLogger();

	private static String configPath = "resources/config/config.properties";

	private ECS ecs = null;
	private InfraMetadata clusterMD = null;
	private ConsistentHash clusterHash = null;
	private ServiceLocation serverMD = null;

	private String serverName = null;
	private int port;
	private int cacheSize;
	private CacheStrategy strategy;
	private ServerSocket serverSocket;
	private boolean running;
	private boolean suspended = true;
	private ServiceLocation serverInfo; // TODO: get the serverInfo

	public static int totalNumClientConnection = 0;
	public static boolean serverOn = false;
	ArrayList<Thread> threadCollection = new ArrayList<>();
	public static ReentrantLock serverLock = new ReentrantLock(); // FIXME:
																	// better
																	// not set
																	// as static
																	// to
																	// improve
																	// performance

	/**
	 * Constructs a (Echo-) Server object which listens to connection attempts
	 * at the given port.
	 * 
	 * @param port
	 *            a port number which the Server is listening to in order to
	 *            establish a socket connection to a client. The port number
	 *            should reside in the range of dynamic ports, i.e 49152 -
	 *            65535.
	 */
	public KVServer() {
		this.cacheSize = -1;
		this.strategy = CacheStrategy.None;
	}

	/**
	 * Start KV Server at given port
	 * 
	 * @param port
	 *            given port for storage server to operate
	 * @param cacheSize
	 *            specifies how many key-value pairs the server is allowed to
	 *            keep in-memory
	 * @param strategy
	 *            specifies the cache replacement strategy in case the cache is
	 *            full and there is a GET- or PUT-request on a key that is
	 *            currently not contained in the cache. Options are "FIFO",
	 *            "LRU", and "LFU".
	 */
	public KVServer(int port, int cacheSize, String strategy) {
		this.port = port;
		this.cacheSize = cacheSize;
		this.strategy = parseCacheStrategy(strategy);
	}

	public void setRunning(boolean running) {
		serverLock.lock();
		this.running = running;
		serverLock.unlock();
	}

	public ServiceLocation getServerInfo() {
		if (serverInfo != null) {
			serverInfo = new ServiceLocation(getServerName(), getHostname(),
					getPort());
		}
		return serverInfo;
	}

	public ECS getECS() {
		return ecs;
	}

	public String getServerName() {
		return serverName;
	}

	public boolean isSuspended() {
		return suspended;
	}

	public void setSuspended(boolean suspended) {
		serverLock.lock();
		this.suspended = suspended;
		serverLock.unlock();
	}

	@Override
	public int getPort() {
		return port;
	}

	@Override
	public String getHostname() {
		if (serverSocket == null) {
			return null;
		} else {
			return serverSocket.getInetAddress().getHostName();
		}
	}

	@Override
	public CacheStrategy getCacheStrategy() {
		return strategy;
	}

	@Override
	public int getCacheSize() {
		return cacheSize;
	}

	@Override
	public boolean inStorage(String key) {
		serverLock.lock();

		try {
			if (inCache(key)) {
				serverLock.unlock();
				return true;
			}
			Disk.getKV(key);
		} catch (Exception e) {
			serverLock.unlock();
			return false;
		}
		serverLock.unlock();
		return true;
	}

	@Override
	public boolean inCache(String key) {
		serverLock.lock();
		boolean inCache = Storage.inCache(key);
		serverLock.unlock();
		return inCache;
	}

	@Override
	public String getKV(String key) throws Exception {
		if (!Disk.if_init()) {
			logger.warn("[ClientConnection]handleGET: DB not initalized during Server startup");
		}

		String value = null;
		try {
			serverLock.lock();
			value = Storage.getKV(key);
		} finally {
			serverLock.unlock();
		}
		return value;
	}

	@Override
	public void putKV(String key, String value) throws Exception {
		if (!Disk.if_init()) {
			logger.warn("[ClientConnection]handlePUT: DB not initalized during Server startup");
			Disk.init(); // FIXME: should raise a warning/error
		}

		try {
			serverLock.lock();
			Storage.putKV(key, value);
		} finally {
			serverLock.unlock();
		}
	}

	@Override
	public void clearCache() {
		serverLock.lock();
		Storage.clearCache();
		serverLock.unlock();
	}

	@Override
	public void clearStorage() {
		serverLock.lock();
		Storage.clearCache();
		Storage.clearStorage();
		serverLock.unlock();
	}

	/**
	 * Initializes and starts the server. Loops until the the server should be
	 * closed.
	 */
	@Override
	public void run() {
		running = initializeServer();

		// Initialize storage units.
		File kvdbFolder = new File("kvdb");
		if (!kvdbFolder.exists()) {
			kvdbFolder.mkdir();
		}
		Disk.setDbName("/kvdb/" + this.serverMD.serviceName + "-kvdb");
		Storage.set_mode(strategy);
		Storage.init(cacheSize);
		serverOn = true;

		ZKConnection zkConnection = new ZKConnection(this);
		Thread newZKConnection = new Thread(zkConnection);
		newZKConnection.start();

		if (serverSocket != null) {
			while (isRunning()) {
				try {
					Socket client = serverSocket.accept();
					ClientConnection connection = new ClientConnection(client,
							this);
					Thread newConnection = new Thread(connection);
					threadCollection.add(newConnection);
					newConnection.start();

					logger.info("Connected to "
							+ client.getInetAddress().getHostName()
							+ " on port " + client.getPort());
				} catch (IOException e) {
					logger.error("Error! "
							+ "Unable to establish connection. \n", e);
				}
			}
		}
		serverOn = false;
		logger.info("Server shutdown.");
	}

	@Override
	public void kill() {
		running = false;
		try {
			Storage.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
		logger.info("Server being killed");
		System.exit(1);
	}

	@Override
	public void close() {
		serverLock.lock();
		running = false;
		try {
			Storage.flush();
			serverSocket.close();
			serverOn = false;
		} catch (IOException e) {
			logger.error("Error! " + "Unable to close socket on port: " + port,
					e);
		} finally {
			serverLock.unlock();
		}
	}

	private boolean isRunning() {
		return this.running;
	}

	private boolean initializeServer() {
		logger.info("Initialize server ...");
		try {
			serverSocket = new ServerSocket(port);
			port = serverSocket.getLocalPort();
			logger.info("Server listening on port: "
					+ serverSocket.getLocalPort());
			return true;
		} catch (IOException e) {
			logger.error("Error! Cannot open server socket:");
			if (e instanceof BindException) {
				logger.error("Port " + port + " is already bound!");
			}
			return false;
		}
	}

	public static CacheStrategy parseCacheStrategy(String str) {
		str = str.toUpperCase();
		if (str.equals("FIFO"))
			return CacheStrategy.FIFO;
		if (str.equals("LRU"))
			return CacheStrategy.LRU;
		if (str.equals("LFU"))
			return CacheStrategy.LFU;
		logger.warn("Invalid cache strategy: " + str
				+ ". Using pure disk storage.");
		return CacheStrategy.None;
	}

	private static void setUpServerLogger() throws Exception {
		Properties props = new Properties();
		props.load(new FileInputStream(
				"resources/config/server-log4j.properties"));
		PropertyConfigurator.configure(props);
	}

	// If key is in the range this server is responsible for.
	public boolean hasKey(String key) {
		return clusterHash.getServer(key).serviceName
				.equals(serverMD.serviceName);
	}

	public InfraMetadata getClusterMD() {
		return clusterMD;
	}

	// Not Thread-safe!
	public void setClusterMD(InfraMetadata newMetadata) {
		this.clusterMD = newMetadata;
		clusterHash = new ConsistentHash();
		clusterHash.addNodesFromInfraMD(newMetadata);
	}

	public void retrieveClusterFromECS(Integer ECS_PORT) throws IOException,
			InterruptedException {
		if (ecs == null) {
			ecs = new ECS();
		}

		ecs.connect("localhost", ECS_PORT);
		clusterMD = ecs.getMD();

		// Compute server side consistent hash.
		clusterHash = new ConsistentHash();
		clusterHash.addNodesFromInfraMD(clusterMD);
	}

	// Compute new hash ring with given metadata, and migrate all storages
	// that no longer belongs to this server. Returns only after all
	// migrations complete. Thread-safe.
	public void migrateWithNewMD(InfraMetadata newMD) throws Exception {
		serverLock.lock();
		Storage.flush();

		// Re-compute new consistent hash ring and provision all keys to
		// migrate.
		setClusterMD(newMD);
		List<String> migrants = new ArrayList<String>();
		for (String key : Disk.getAllKeys()) {
			// Key stays on this server.
			if (clusterHash.getServer(key).serviceName
					.equals(serverMD.serviceName)) {
				continue;
			}
			migrants.add(key);
		}

		// Remove and send all migrating keys.
		logger.info("Say goodbye to " + migrants);
		ConnectionUtil conn = new ConnectionUtil();
		for (String key : migrants) {
			// Construct target and server message.
			CommMessage msg = new CommMessageBuilder().setKey(key)
					.setValue(Disk.getKV(key)).setStatus(StatusType.PUT)
					.build();
			msg.setFromServer(true);
			ServiceLocation target = clusterHash.getServer(key);

			// Send and await ack.
			Socket socket = new Socket(target.host, target.port);
			conn.sendCommMessage(socket.getOutputStream(), msg);
			if (conn.receiveCommMessage(socket.getInputStream()).getStatus() != StatusType.PUT_SUCCESS) {
				logger.error("Error migrating message " + msg + " from server "
						+ serverName + " to server " + target.serviceName);
			}
			socket.close();

			// Remove local copy.
			Disk.putKV(key, null);
		}
		serverLock.unlock();
	}

	public static KVServer initServerFromECS(String[] args) throws Exception {
		// Coordinate and initialize server w.r.t ECS metadata.
		// Initialize clusterMD and compute clusterHash.
		KVServer server = new KVServer();
		server.retrieveClusterFromECS(Integer.parseInt(args[0]));

		// Calculate server instance specific metadata.
		server.serverMD = server.clusterMD.locationOfService(args[1]);
		server.port = server.serverMD.port;
		server.serverName = server.serverMD.serviceName;

		server.cacheSize = Integer.parseInt(args[2]);
		server.strategy = parseCacheStrategy(args[3]);

		return server;
	}

	/**
	 * Main entry point for the echo server application.
	 * 
	 * @param args
	 *            contains the port number at args[0].
	 */
	public static void main(String[] args) throws Exception {
		try {
			try {
				setUpServerLogger();
			} catch (Exception e) {
				System.out
						.println("Unable to read from resources/config/server-log4j.properties");
				System.out.println("Using default logger from skeleton code.");
				new LogSetup("logs/server-default.log", Level.ALL);
			}

			if (args.length != 4) {
				System.out.println("Error! Invalid number of arguments!");
				System.out
						.println("Usage: Server <port>! || Server <ECS_port> <serverName> <cacheSize> <cacheStrategy>");
				return;
			}
			KVServer server = initServerFromECS(args);
			System.out.println("Service: " + server.serverMD.serviceName
					+ " will listen on " + server.serverMD.host + ":"
					+ server.serverMD.port);

			// Start server.
			server.start();
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize server!");
			e.printStackTrace();
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: Server <port>!");
		}
	}
}
