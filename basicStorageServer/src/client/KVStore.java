package client;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import app_kvClient.ClientSocketListener;
import app_kvClient.ClientSocketListener.SocketStatus;
import shared.ConnectionUtil;
import shared.messages.CommMessage;
import shared.messages.CommMessageBuilder;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

public class KVStore extends Thread implements KVCommInterface {

	private Logger logger = Logger.getRootLogger();
	private Set<ClientSocketListener> listeners;
	private boolean running;

	private Socket clientSocket;
	private String address;
	private int port;

	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
	}

	/**
	 * Initializes and starts the client connection. Loops until the connection
	 * is closed or aborted by the client.
	 */
	public void run() {
		try {
			clientSocket.getOutputStream();
			clientSocket.getInputStream();
			ConnectionUtil conn = new ConnectionUtil();
			while (isRunning()) {
				try {
					CommMessage latestMsg = conn
							.receiveCommMessage(clientSocket.getInputStream());
					for (ClientSocketListener listener : listeners) {
						listener.handleNewCommMessage(latestMsg);
					}
				} catch (IOException ioe) {
					if (isRunning()) {
						logger.error("Connection lost!");
						try {
							tearDownConnection();
							for (ClientSocketListener listener : listeners) {
								listener.handleStatus(SocketStatus.CONNECTION_LOST);
							}
						} catch (IOException e) {
							logger.error("Unable to close connection!");
						}
					}
				}
			}
		} catch (IOException ioe) {
			logger.error("Connection could not be established!");

		} finally {
			if (isRunning()) {
				closeConnection();
			}
		}
	}

	public synchronized void closeConnection() {
		logger.info("try to close connection ...");

		try {
			tearDownConnection();
			for (ClientSocketListener listener : listeners) {
				listener.handleStatus(SocketStatus.DISCONNECTED);
			}
		} catch (IOException ioe) {
			logger.error("Unable to close connection!");
		}
	}

	private void tearDownConnection() throws IOException {
		setRunning(false);
		logger.info("tearing down the connection ...");
		if (clientSocket != null) {
			clientSocket.close();
			clientSocket = null;
			logger.info("connection closed!");
		}
	}

	public boolean isRunning() {
		return running;
	}

	public void setRunning(boolean run) {
		running = run;
	}

	public void addListener(ClientSocketListener listener) {
		listeners.add(listener);
	}

	/**
	 * Method sends a PUT message using this socket.
	 * 
	 * @param msg
	 *            the message that is to be sent.
	 * @throws IOException
	 *             some I/O error regarding the output stream
	 */

	@Override
	public void connect() throws UnknownHostException, IOException {
		clientSocket = new Socket(address, port);
		listeners = new HashSet<ClientSocketListener>();
		setRunning(true);
		logger.info("Connection established");
	}

	@Override
	public void disconnect() {
		closeConnection();
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		try {
			CommMessage cm = new CommMessageBuilder()
					.setStatus(KVMessage.StatusType.PUT).setKey(key)
					.setValue(value).build();
			ConnectionUtil conn = new ConnectionUtil();
			conn.sendCommMessage(clientSocket.getOutputStream(), cm);

			CommMessage latestMsg = conn.receiveCommMessage(clientSocket
					.getInputStream());
			return latestMsg;
		} catch (IOException ioe) {
			if (isRunning()) {
				logger.error("Connection lost!");
				try {
					tearDownConnection();
					for (ClientSocketListener listener : listeners) {
						listener.handleStatus(SocketStatus.CONNECTION_LOST);
					}
				} catch (IOException e) {
					logger.error("Unable to close connection!");
				}
			}
		}
		return null;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		try {
			CommMessage cm = new CommMessage(StatusType.GET, key.toString(),
					null);
			ConnectionUtil conn = new ConnectionUtil();
			conn.sendCommMessage(clientSocket.getOutputStream(), cm);

			CommMessage latestMsg = conn.receiveCommMessage(clientSocket
					.getInputStream());
			
			System.out.println(latestMsg.getValue());
			return latestMsg;
		} catch (IOException ioe) {
			logger.error("Connection lost!");
			try {
				tearDownConnection();
				for (ClientSocketListener listener : listeners) {
					listener.handleStatus(SocketStatus.CONNECTION_LOST);
				}
			} catch (IOException e) {
				logger.error("Unable to close connection!");
			}
		}
		return null;
	}
}
