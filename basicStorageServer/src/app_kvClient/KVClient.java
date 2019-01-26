package app_kvClient;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import app_kvServer.KVServer;

import shared.messages.CommMessage;

import logger.LogSetup;
import client.KVCommInterface;
import client.KVStore;

public class KVClient implements IKVClient, ClientSocketListener {

	private static Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "m1-client> ";
	private BufferedReader stdin;
	private KVStore backend = null;
	private boolean stop = false;

	private String serverAddress;
	private int serverPort;

	@Override
	public void newConnection(String hostname, int port)
			throws UnknownHostException, IOException {
		backend = new KVStore(hostname, port);
		backend.connect();
		backend.addListener(this);
	}

	// Disconnet from the current backend connection.
	private void disconnect() {
		if (backend != null) {
			backend.closeConnection();
			backend = null;
		}
	}

	/**
	 * Current not using singleton store object.
	 */
	@Override
	public KVCommInterface getStore() {
		return backend;
	}

	/*
	 * Copied from echoClient
	 */
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
			disconnect();
			System.out.println(PROMPT + "Application exit!");
			break;

		case "connect":
			if (tokens.length == 3) {
				try {
					serverAddress = tokens[1];
					serverPort = Integer.parseInt(tokens[2]);
					newConnection(serverAddress, serverPort);
				} catch (NumberFormatException nfe) {
					printError("No valid address. Port must be a number!");
					logger.info("Unable to parse argument <port>", nfe);
				} catch (UnknownHostException e) {
					printError("Unknown Host!");
					logger.info("Unknown Host!", e);
				} catch (IOException e) {
					printError("Could not establish connection!");
					logger.warn("Could not establish connection!", e);
				}
			} else {
				printError("Invalid number of parameters!");
			}
			break;

		case "disconnect":
			disconnect();
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
			System.out.println("[debug]m1-client put: tokens size: "
					+ tokens.length);
			if (tokens.length < 2) {
				printError("Incorrect num of arguments; Must be passing in as: put <key> <value>; or put <key> for deletion");
				return;
			}
			if (tokens.length > 3) {
				tokens = parsePutInput(tokens);
			}
			if (backend == null || !backend.isRunning()) {
				printError("Not connected!");
				return;
			}
			try {
				if (tokens.length == 2) {
					backend.put(tokens[1], null);
				} else {
					backend.put(tokens[1], tokens[2]);
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
			if (backend != null && backend.isRunning()) {
				try {
					backend.get(tokens[1]);
				} catch (Exception e) {
					printError("Error getting key " + tokens[1] + ": "
							+ e.toString());
				}
			}
			break;

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
		sb.append(PROMPT).append("put <key>");
		sb.append("\t\t get the value corresponding to the key \n");
		sb.append(PROMPT).append("disconnect");
		sb.append("\t\t\t disconnects from the server \n");

		sb.append(PROMPT).append("logLevel");
		sb.append("\t\t\t changes the logLevel \n");
		sb.append(PROMPT).append("\t\t\t\t ");
		sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

		sb.append(PROMPT).append("quit ");
		sb.append("\t\t\t exits the program");
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

	@Override
	public void handleNewCommMessage(CommMessage cm) {
		if (!stop) {
			System.out.println(cm.toString());
			System.out.print(PROMPT);
		}

	}

	@Override
	public void handleStatus(SocketStatus status) {
		if (status == SocketStatus.CONNECTED) {

		} else if (status == SocketStatus.DISCONNECTED) {
			System.out.print(PROMPT);
			System.out.println("Connection terminated: " + serverAddress
					+ " / " + serverPort);

		} else if (status == SocketStatus.CONNECTION_LOST) {
			System.out.println("Connection lost: " + serverAddress + " / "
					+ serverPort);
			System.out.print(PROMPT);
		}
	}

	private void printError(String error) {
		logger.error(PROMPT + "Error! " + error);
	}

	private static void setUpClientLogger() throws Exception {
		Properties props = new Properties();
		props.load(new FileInputStream("resources/config/client-log4j.properties"));
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
			try {
				setUpClientLogger();
			} catch (Exception e) {
				System.out
						.println("Unable to read from resources/config/client-log4j.properties");
				System.out.println("Using default logger from skeleton code.");
				new LogSetup("logs/client-default.log", Level.ALL);
			}
			KVClient app = new KVClient();
			app.run();
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}
	}

}
