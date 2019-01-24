package app_kvServer;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvServer.storage.Disk;
import app_kvServer.storage.FIFOCache;
import app_kvServer.storage.LFUCache;
import app_kvServer.storage.LRUCache;
import app_kvServer.storage.Storage;

public class KVServer extends Thread implements IKVServer {
	private static Logger logger = Logger.getRootLogger();

	private static String configPath = "resources/config/config.properties";

	private int port;
	private int cacheSize;
	private CacheStrategy strategy;
	private ServerSocket serverSocket;
	private boolean running;

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
	public KVServer(int port) {
		this.port = port;
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
		try {
			if (inCache(key)) {
				return true;
			}
			Disk.getKV(key);
		} catch (Exception e) {
			return false;
		}
		
		return true;
	}

	@Override
	public boolean inCache(String key) {
		return Storage.inCache(key);
	}

	@Override
	public String getKV(String key) throws Exception {
		if (!Disk.if_init()) {
			logger.warn("[ClientConnection]handleGET: DB not initalized during Server startup");
		}

		return Storage.getKV(key);
	}

	@Override
	public void putKV(String key, String value) throws Exception {
		if (!Disk.if_init()) {
			logger.warn("[ClientConnection]handlePUT: DB not initalized during Server startup");
			Disk.init(); //FIXME: should raise a warning/error
		}

		System.out.println("[KVServer.java; putKV]key: " + key + " value: " + value);
		Storage.putKV(key, value);
	}

	@Override
	public void clearCache() {
		System.out.println("[KVServer.java]Enter clearCache");
		Storage.clearCache();
		
	}

	@Override
	public void clearStorage() {
		System.out.println("[KVServer.java]Enter clearStorage");
		Storage.clearCache();
		Storage.clearStorage();
	}

	/**
	 * Initializes and starts the server. Loops until the the server should be
	 * closed.
	 */
	@Override
	public void run() {
		running = initializeServer();
		Storage.set_mode(strategy);
		Storage.init(cacheSize);
		
		//DELETME: 
//		Storage.clearStorage();
//		kill();
		

		if (serverSocket != null) {
			while (isRunning()) {
				try {
					Socket client = serverSocket.accept();
					ClientConnection connection = new ClientConnection(client);
					new Thread(connection).start();

					logger.info("Connected to "
							+ client.getInetAddress().getHostName()
							+ " on port " + client.getPort());
				} catch (IOException e) {
					logger.error("Error! "
							+ "Unable to establish connection. \n", e);
				}
			}
		}
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
		} catch (IOException e) {
			logger.error("Error! " + "Unable to close socket on port: " + port,
					e);
		}
	}

	private boolean isRunning() {
		return this.running;
	}

	/**
	 * Stops the server insofar that it won't listen at the given port any more.
	 */
	public void stopServer() {
		running = false;
		try {
			serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! " + "Unable to close socket on port: " + port,
					e);
		}
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
		
		switch (str) {
			case "FIFO":
				return CacheStrategy.FIFO;
			case "LRU":
				return CacheStrategy.LRU;
			case "LFU":
				return CacheStrategy.LFU;
			default:
				return CacheStrategy.None;
		}
	}

	/**
	 * Main entry point for the echo server application.
	 * 
	 * @param args
	 *            contains the port number at args[0].
	 */
	public static void main(String[] args) {
		try {
			new LogSetup("logs/server.log", Level.ALL);
			
			if (args.length != 1) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port>!");
			} else {
				int port = Integer.parseInt(args[0]);
				KVServer server = new KVServer(port);

				System.out.println("Working Directory = " +
			              System.getProperty("user.dir"));
				
				// Read configuration file for cache & server configs.
				Properties props = new Properties();
				props.load(new FileInputStream(configPath));
				server.cacheSize = Integer.parseInt(props
						.getProperty("cache_limit"));
				server.strategy = parseCacheStrategy(props
						.getProperty("cache_policy"));

				// Start server.
				server.start();
			}
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
