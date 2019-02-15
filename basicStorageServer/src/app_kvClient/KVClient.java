package app_kvClient;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.util.Properties;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import shared.ConnectionUtil;
import shared.InfraMetadata.ServiceLocation;
import shared.messages.CommMessage;
import shared.messages.KVAdminMessage;
import shared.messages.KVAdminMessage.KVAdminMessageType;
import shared.messages.KVMessage.StatusType;
import app_kvServer.KVServer;
import client.KVCommInterface;
import client.KVStore;

public class KVClient implements IKVClient {

	private static Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "m1-client> ";
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
		
			
		//only for testing purposes
//		case "shutdown":
//			try {
//				CommMessage cm = new CommMessage(StatusType.SERVER_STOPPED, null, null);
//				cm.setAdminMessage(new KVAdminMessage());
//				cm.getAdminMessage().setKVAdminMessageType(KVAdminMessageType.SHUTDOWN);
//				ConnectionUtil conn = new ConnectionUtil();
//				conn.sendCommMessage(backend.clientSocket.getOutputStream(), cm);
//				//CommMessage latestMsg = conn.receiveCommMessage(backend.clientSocket.getInputStream());
//				
//				System.out.println("Serve should be closed now");
//			} catch (IOException ioe) {
//				logger.error("Connection lost!");
//			}
//			break;
//		
//		//only for testing purposes
//		case "stop":
//			//Stop all the servers (should be issued from ECS)
//			try {
//				CommMessage cm = new CommMessage(StatusType.SERVER_STOPPED, null, null);
//				cm.setAdminMessage(new KVAdminMessage());
//				cm.getAdminMessage().setKVAdminMessageType(KVAdminMessageType.STOP);
//				ConnectionUtil conn = new ConnectionUtil();
//				conn.sendCommMessage(backend.clientSocket.getOutputStream(), cm);
//				//CommMessage latestMsg = conn.receiveCommMessage(backend.clientSocket.getInputStream());
//				
//				System.out.println("Serve should be closed now");
//			} catch (IOException ioe) {
//				logger.error("Connection lost!");
//			}
//			break;
//			
//		case "start":
//			//Start all the servers (should be issued from ECS)
//			try {
//				CommMessage cm = new CommMessage(StatusType.SERVER_STARTED, null, null);
//				cm.setAdminMessage(new KVAdminMessage());
//				cm.getAdminMessage().setKVAdminMessageType(KVAdminMessageType.START);
//				ConnectionUtil conn = new ConnectionUtil();
//				conn.sendCommMessage(backend.clientSocket.getOutputStream(), cm);
//				//CommMessage latestMsg = conn.receiveCommMessage(backend.clientSocket.getInputStream());
//				
//				System.out.println("Serve should be started now");
//			} catch (IOException ioe) {
//				logger.error("Connection lost!");
//			}
//			break;
			
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
