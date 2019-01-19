package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import shared.messages.CommMessage;

import logger.LogSetup;
import client.ClientSocketListener;
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
	public void newConnection(String hostname, int port) throws Exception {
		// TODO Auto-generated method stub
	}

	/**
	 * Current not using singleton store object.
	 */
	@Override
	public KVCommInterface getStore() {
		// TODO Auto-generated method stub
		return null;
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
					connect(serverAddress, serverPort);
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
			if (tokens.length != 3) {
				printError("Incorrect num of arguments; Must be passing in as: put <key> <value> pairs");
				return;
			}
			if (backend == null || !backend.isRunning()) {
				printError("Not connected!");
				return;
			}
			try {
				backend.put(tokens[1], tokens[2]);
			} catch (Exception e) {
				printError("Put Error: " + e.toString());
			}
			break;

		// case "get":

		default:
			printError("Unknown command");
			printHelp();
			return;
		}

		// else if (tokens[0].equals("get")) {
		// if (tokens.length == 2) {
		// if (client != null && client.isRunning()) {
		// StringBuilder key = new StringBuilder();
		// key.append(tokens[1]);
		//
		// //FIXME: may be incorrect to set the value to null
		// CommMessage cm = new CommMessage(StatusType.GET, OptCode.GET,
		// key.toString(), null);
		// sendCommMessage(cm);
		// }
		// } else {
		// printError("Incorrect num of arguments; Must be passing in as: get <key>");
		// }
	}

	private void connect(String address, int port) throws UnknownHostException,
			IOException {
		backend = new KVStore(address, port);
		backend.connect();
		backend.addListener(this);
	}

	private void disconnect() {
		if (backend != null) {
			backend.closeConnection();
			backend = null;
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
		sb.append(PROMPT).append("send <text message>");
		sb.append("\t\t sends a text message to the server \n");
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
		System.out.println(PROMPT + "Error! " + error);
	}

	/**
	 * Main entry point for the echo server application.
	 * 
	 * @param args
	 *            contains the port number at args[0].
	 */
	public static void main(String[] args) {
		try {
			new LogSetup("logs/client.log", Level.OFF);
			KVClient app = new KVClient();
			app.setLevel(Level.ALL.toString());
			app.run();
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}
	}

}
