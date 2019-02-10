package app_kvServer;

import java.io.FileInputStream;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;
import java.util.UUID;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import shared.ConnectionUtil;
import shared.InfraMetadata;
import shared.InfraMetadata.ServiceLocation;
import shared.messages.CommMessage;
import shared.messages.CommMessageBuilder;
import shared.messages.KVMessage.StatusType;

import app_kvServer.storage.Disk;
import app_kvServer.storage.Storage;

public class KVServer extends Thread implements IKVServer {
	private static Logger logger = Logger.getRootLogger();

	private static String configPath = "resources/config/config.properties";

	private InfraMetadata cluster = null;
	private ServiceLocation serverMetadata = null;

	private int port;
	private int cacheSize;
	private CacheStrategy strategy;
	private ServerSocket serverSocket;
	private boolean running;

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
		return IKVServer.CacheStrategy.None;
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
		Disk.setDbName("/" + this.serverMetadata.serviceName + "-kvdb");
		Storage.set_mode(strategy);
		Storage.init(cacheSize);
		serverOn = true;

		if (serverSocket != null) {
			while (isRunning()) {
				try {
					Socket client = serverSocket.accept();
					ClientConnection connection = new ClientConnection(client);
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
		logger.info("Server stopped.");
	}

	@Override
	public void kill() {
		running = false;
		logger.info("Server being killed");
		System.exit(1);
	}

	@Override
	public void close() {
		running = false;
		try {
			Storage.flush();
			serverSocket.close();
			serverOn = false;
		} catch (IOException e) {
			logger.error("Error! " + "Unable to close socket on port: " + port,
					e);
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

	public static KVServer initServerFromECS(String[] args) throws Exception {
		// Request ECS for cluster metadata.
		CommMessage clusterRequest = new CommMessageBuilder()
				.setStatus(StatusType.GET).setKey("METADATA").build();
		ConnectionUtil conn = new ConnectionUtil();
		Socket ecsSocket = new Socket("localhost", Integer.parseInt(args[0]));
		conn.sendCommMessage(ecsSocket.getOutputStream(), clusterRequest);
		CommMessage clusterInfo = conn.receiveCommMessage(ecsSocket
				.getInputStream());
		ecsSocket.close();

		// Coordinate and initialize server within cluster metadata.
		KVServer server = new KVServer();
		server.cluster = clusterInfo.getInfraMetadata();
		server.serverMetadata = server.cluster.locationOfService(args[1]);
		server.port = server.serverMetadata.port;

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

			if (args.length > 2) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port>!");
				return;
			}
			KVServer server = null;
			if (args.length == 2) {
				server = initServerFromECS(args);
			} else {
				if (args.length == 0) {
					System.out.println("[Error]Missing port number");
					System.exit(1);
				}
				// No ECS startup. Mock cluster metadata.
				server = new KVServer();
				server.port = Integer.parseInt(args[0]);
				server.serverMetadata = new ServiceLocation(UUID.randomUUID()
						.toString(), "127.0.0.1", server.port);
				server.cluster = new InfraMetadata();// clusterInfo.getInfraMetadata();
				server.cluster.getServerLocations().add(server.serverMetadata);
			}

			System.out.println("Service: " + server.serverMetadata.serviceName
					+ " will listen on " + server.serverMetadata.host + ":"
					+ server.serverMetadata.port);

			// Read configuration file for cache & server configs.
			Properties props = new Properties();
			props.load(new FileInputStream(configPath));
			server.cacheSize = Integer.parseInt(props
					.getProperty("cache_limit"));
			server.strategy = parseCacheStrategy(props
					.getProperty("cache_policy"));

			// Start server.
			server.start();
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize server!");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: Server <port>!");
			System.exit(1);
		}
	}
}
