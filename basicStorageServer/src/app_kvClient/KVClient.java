package app_kvClient;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.UUID;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import shared.messages.CommMessage;
import shared.messages.KVMessage.StatusType;
import app_kvServer.KVServer;
import client.KVCommInterface;
import client.KVStore;

public class KVClient implements IKVClient {

	private static Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "m1-client> ";
	private static final int RETRY_LIMIT = 10; // Controls how many times client
												// should retry if receives
												// SERVER_NOT_RESPONSIBLE.
	private BufferedReader stdin;
	private KVStore backend = null;
	private boolean stop = false;

	public KVClient() throws IOException, InterruptedException {
		backend = new KVStore();
	}

	@Override
	public void newConnection(String hostname, int port)
			throws UnknownHostException, IOException, InterruptedException {
	}

	/**
	 * Current not using singleton store object.
	 */
	@Override
	public KVCommInterface getStore() {
		return backend;
	}

	public void run() {
		while (!stop) {
			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(PROMPT);

			try {
				String cmdLine = stdin.readLine();
				this.handleCommand(cmdLine);
			} catch (IOException e) {
				stop = true;
				printError("CLI does not respond - Application terminated ");
			}
		}
	}

	private String[] parsePutInput(String[] input) {
		String[] result = new String[3];
		result[0] = input[0];
		result[1] = input[1];
		StringBuilder sb = new StringBuilder();
		for (int i = 2; i < input.length; i++) {
			sb.append(input[i] + " ");
		}
		sb.deleteCharAt(sb.length() - 1);
		result[2] = sb.toString();
		return result;
	}

	private void handleCommand(String cmdLine) {
		String[] tokens = cmdLine.split("\\s+");

		switch (tokens[0]) {

		case "quit":
			stop = true;
			System.out.println(PROMPT + "Application exit!");
			break;

		case "logLevel":
			if (tokens.length == 2) {
				String level = setLevel(tokens[1]);
				if (level.equals(LogSetup.UNKNOWN_LEVEL)) {
					printError("No valid log level!");
					printPossibleLogLevels();
				} else {
					System.out.println(PROMPT + "Log level changed to level "
							+ level);
				}
			} else {
				printError("Invalid number of parameters!");
			}
			break;

		case "help":
			printHelp();
			break;

		case "connect":
			if (tokens.length != 3) {
				printError("Incorrect num of arguments; "
						+ "Must be passing in as: connect <address> <port>");
				return;
			}
			if (backend.isRunning()) {
				printError("Please disconnect first");
				return;
			}
			backend.setConnectTarget(tokens[1], Integer.parseInt(tokens[2]));
			try {
				backend.connect();
			} catch (IOException e2) {
				printError("Error executing connect command: " + tokens + "\n"
						+ e2);
			}
			System.out.println("Connection established!");
			break;

		case "disconnect":
			backend.disconnect();
			System.out.println("Connect session ended");

		case "put":
			if (tokens.length < 2) {
				printError("Incorrect num of arguments; "
						+ "Must be passing in as: put <key> <value> for insertion; "
						+ "or put <key> for deletion");
				return;
			}

			if (tokens.length > 3) {
				tokens = parsePutInput(tokens);
			}

			try {
				CommMessage latestMsg = null;
				String key = tokens[1];
				String value = null;

				if (tokens.length == 2) {
					value = null;
				} else {
					value = tokens[2];
				}

				latestMsg = (CommMessage) backend.put(key, value);
				// getting a metaData file from the server
				// need to do a retry on the corresponding server
				int i = 0;
				while (latestMsg != null
						&& latestMsg.getStatus().equals(
								StatusType.SERVER_NOT_RESPONSIBLE)
						&& latestMsg.getInfraMetadata() != null) {
					// FIXME: should we just retry once???
					System.out.println("[debug]Round " + i);
					i++;

					backend.resetClusterHash(latestMsg.getInfraMetadata());
					latestMsg = (CommMessage) backend.put(key, value);
				}
			} catch (Exception e) {
				printError("Put Error: " + e.toString());
			}
			break;

		case "get":
			if (tokens.length != 2) {
				printError("Incorrect num of arguments; Must be passing in as: get <key>");
				return;
			}
			try {
				String key = tokens[1];
				CommMessage latestMsg = (CommMessage) backend.get(key);

				// getting a metaData file from the server
				// need to do a retry on the corresponding server
				int i = 0;
				while (latestMsg != null
						&& latestMsg.getStatus().equals(
								StatusType.SERVER_NOT_RESPONSIBLE)
						&& latestMsg.getInfraMetadata() != null) {
					// FIXME: should we just retry once???
					System.out.println("[debug]Round " + i);
					i++;

					backend.resetClusterHash(latestMsg.getInfraMetadata());
					latestMsg = (CommMessage) backend.get(key);
				}
			} catch (Exception e) {
				printError("Error getting key " + tokens[1] + ": "
						+ e.toString());
			}

			break;
		case "runScript":
			File file = new File(tokens[1]);

			BufferedReader br;
			try {
				br = new BufferedReader(new FileReader(file));
				String st;
				try {
					while ((st = br.readLine()) != null) {
						String lines[] = st.split("\\s+");
						if (lines[0].equals("get")) {
							get(lines[1]);
						} else if (lines[0].equals("put")) {
							if (lines.length >= 3) {
								int i = 2;
								String value = "";
								while (i < lines.length) {
									value += lines[i];
									i++;
								}
								put(lines[1], value);
							}
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			} catch (FileNotFoundException e1) {
				e1.printStackTrace();
			}

			break;

		// only for testing purposes
		// case "shutdown":
		// try {
		// CommMessage cm = new CommMessage(StatusType.SERVER_STOPPED, null,
		// null);
		// cm.setAdminMessage(new KVAdminMessage());
		// cm.getAdminMessage().setKVAdminMessageType(KVAdminMessageType.SHUTDOWN);
		// ConnectionUtil conn = new ConnectionUtil();
		// conn.sendCommMessage(backend.clientSocket.getOutputStream(), cm);
		// //CommMessage latestMsg =
		// conn.receiveCommMessage(backend.clientSocket.getInputStream());
		//
		// System.out.println("Serve should be closed now");
		// } catch (IOException ioe) {
		// logger.error("Connection lost!");
		// }
		// break;
		//
		// //only for testing purposes
		// case "stop":
		// //Stop all the servers (should be issued from ECS)
		// try {
		// CommMessage cm = new CommMessage(StatusType.SERVER_STOPPED, null,
		// null);
		// cm.setAdminMessage(new KVAdminMessage());
		// cm.getAdminMessage().setKVAdminMessageType(KVAdminMessageType.STOP);
		// ConnectionUtil conn = new ConnectionUtil();
		// conn.sendCommMessage(backend.clientSocket.getOutputStream(), cm);
		// //CommMessage latestMsg =
		// conn.receiveCommMessage(backend.clientSocket.getInputStream());
		//
		// System.out.println("Serve should be closed now");
		// } catch (IOException ioe) {
		// logger.error("Connection lost!");
		// }
		// break;
		//
		// case "start":
		// //Start all the servers (should be issued from ECS)
		// try {
		// CommMessage cm = new CommMessage(StatusType.SERVER_STARTED, null,
		// null);
		// cm.setAdminMessage(new KVAdminMessage());
		// cm.getAdminMessage().setKVAdminMessageType(KVAdminMessageType.START);
		// ConnectionUtil conn = new ConnectionUtil();
		// conn.sendCommMessage(backend.clientSocket.getOutputStream(), cm);
		// //CommMessage latestMsg =
		// conn.receiveCommMessage(backend.clientSocket.getInputStream());
		//
		// System.out.println("Serve should be started now");
		// } catch (IOException ioe) {
		// logger.error("Connection lost!");
		// }
		// break;

		default:
			printError("Unknown command");
			printHelp();
			return;
		}
	}

	private void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append(PROMPT).append("M1 CLIENT HELP (Usage):\n");
		sb.append(PROMPT);
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append(PROMPT).append("connect <host> <port>");
		sb.append("\t establishes a connection to a server\n");
		sb.append(PROMPT).append("put <key> <value | null>");
		sb.append("\t\t put/update/delete a key-value pair \n");
		sb.append(PROMPT).append("get <key>");
		sb.append("\t\t get the value corresponding to the key \n");
		sb.append(PROMPT).append("disconnect");
		sb.append("\t\t\t disconnects from the server \n");

		sb.append("logLevel");
		sb.append("\t\t\t changes the logLevel. Available levels: {ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF}\n");

		sb.append("quit ");
		sb.append("\t\t\t\t exits the program");
		sb.append("\n************************************************************************************\n");
		System.out.println(sb.toString());
	}

	private void printPossibleLogLevels() {
		System.out.println(PROMPT + "Possible log levels are:");
		System.out.println(PROMPT
				+ "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
	}

	private String setLevel(String levelString) {

		if (levelString.equals(Level.ALL.toString())) {
			logger.setLevel(Level.ALL);
			return Level.ALL.toString();
		} else if (levelString.equals(Level.DEBUG.toString())) {
			logger.setLevel(Level.DEBUG);
			return Level.DEBUG.toString();
		} else if (levelString.equals(Level.INFO.toString())) {
			logger.setLevel(Level.INFO);
			return Level.INFO.toString();
		} else if (levelString.equals(Level.WARN.toString())) {
			logger.setLevel(Level.WARN);
			return Level.WARN.toString();
		} else if (levelString.equals(Level.ERROR.toString())) {
			logger.setLevel(Level.ERROR);
			return Level.ERROR.toString();
		} else if (levelString.equals(Level.FATAL.toString())) {
			logger.setLevel(Level.FATAL);
			return Level.FATAL.toString();
		} else if (levelString.equals(Level.OFF.toString())) {
			logger.setLevel(Level.OFF);
			return Level.OFF.toString();
		} else {
			return LogSetup.UNKNOWN_LEVEL;
		}
	}

	private void printError(String error) {
		logger.error(PROMPT + "Error! " + error);
	}

	private static void setUpClientLogger() throws Exception {
		Properties props = new Properties();
		props.load(new FileInputStream(
				"resources/config/client-log4j.properties"));
		PropertyConfigurator.configure(props);
	}

	public boolean shoudRetry(CommMessage msg) {
		return msg != null
				&& msg.getStatus().equals(StatusType.SERVER_NOT_RESPONSIBLE)
				&& msg.getInfraMetadata() != null;
	}

	public void get(String key) {
		try {
			CommMessage latestMsg = (CommMessage) backend.get(key);

			// getting a metaData file from the server
			// need to do a retry on the corresponding server
			int i = 0;
			while (shoudRetry(latestMsg) && i < RETRY_LIMIT) {
				i++;
				logger.info("Client received message: " + latestMsg);
				logger.info("Client retrying with new metadata in response");

				backend.resetClusterHash(latestMsg.getInfraMetadata());
				latestMsg = (CommMessage) backend.get(key);
			}
		} catch (Exception e) {
			printError("Error getting key " + key + ": " + e.toString());
		}
	}

	public void put(String key, String value) {
		try {
			CommMessage latestMsg = null;

			latestMsg = (CommMessage) backend.put(key, value);
			// getting a metaData file from the server
			// need to do a retry on the corresponding server
			int i = 0;
			while (shoudRetry(latestMsg) && i < RETRY_LIMIT) {
				i++;
				logger.info("Client received message: " + latestMsg);
				logger.info("Client retrying with new metadata in response");

				backend.resetClusterHash(latestMsg.getInfraMetadata());
				latestMsg = (CommMessage) backend.put(key, value);
			}
		} catch (Exception e) {
			printError("Put Error: " + e.toString());
		}
	}

	private static void testRandomPuts(Integer num) throws Exception {
		KVStore backend = new KVStore();
		while (num > 0) {
			num--;
			String data = UUID.randomUUID().toString().substring(0, 15);
			if (((CommMessage) backend.put(data + num, data + num)).getStatus() != StatusType.PUT_SUCCESS) {
				System.out.println("Error PUT");
			}
		}
	}

	public static void runScript(String fileName) throws Exception {
		File file = new File(fileName);
		KVStore backend = new KVStore();

		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(file));
			String st;
			try {
				while ((st = br.readLine()) != null) {
					String lines[] = st.split("\\s+");
					if (lines[0].equals("get")) {
						backend.get(lines[1]);
					} else if (lines[0].equals("put")) {
						if (lines.length >= 3) {
							int i = 2;
							String value = "";
							while (i < lines.length) {
								value += lines[i];
								i++;
							}
							backend.put(lines[1], value);
						}
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
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
			// Hack shared ConnectionUtil interrupt between server and client
			// code.
			KVServer.serverOn = true;
			KVClient app = null;
			boolean isInteger = true;

			if (args.length > 0) {
				try {
					Integer.parseInt(args[0]);
				} catch (NumberFormatException e) {
					isInteger = false;
				}

				if (!isInteger) {
					try {
						runScript(args[0]);
					} catch (Exception e) {
						System.out.println("[KVClient]runScrip failed!");
						e.printStackTrace();
					}
					return;
				} else {
					try {
						testRandomPuts(Integer.parseInt(args[0]));
					} catch (Exception e) {
						System.out
								.println("Usage: java -jar client.jar <number of put requests>");
					}
					return;
				}
			}

			try {
				app = new KVClient();
			} catch (Exception e1) {
				logger.error("Error initializing KVStore(asking ECS for metadata)");
				e1.printStackTrace();
				return;
			}

			try {
				setUpClientLogger();
			} catch (Exception e) {
				System.out
						.println("Unable to read from resources/config/client-log4j.properties");
				System.out.println("Using default logger from skeleton code.");
				new LogSetup("logs/client-default.log", Level.ALL);
			}
			app.run();
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			return;
		}
	}

}
