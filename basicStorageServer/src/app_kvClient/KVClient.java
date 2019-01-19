package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import shared.messages.CommMessage;
import shared.messages.CommMessage.OptCode;
import shared.messages.KVMessage.StatusType;

import logger.LogSetup;
import client.KVCommInterface;

public class KVClient implements IKVClient, ClientSocketListener {
	
	private static Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "m1-client> ";
	private BufferedReader stdin;
	private Client client = null;
	private boolean stop = false;
	
	private String serverAddress;
	private int serverPort;
	
	
    @Override
    public void newConnection(String hostname, int port) throws Exception{
        // TODO Auto-generated method stub
    }

    @Override
    public KVCommInterface getStore(){
        // TODO Auto-generated method stub
        return null;
    }
    

    /*
     * Copied from echoClient
     */
	public void run() {
		while(!stop) {
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

		if(tokens[0].equals("quit")) {	
			stop = true;
			disconnect();
			System.out.println(PROMPT + "Application exit!");
		
		} else if (tokens[0].equals("connect")){
			if(tokens.length == 3) {
				try{
					serverAddress = tokens[1];
					serverPort = Integer.parseInt(tokens[2]);
					connect(serverAddress, serverPort);
				} catch(NumberFormatException nfe) {
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
			
		}
		
//		else if (tokens[0].equals("send")) {
//			if(tokens.length >= 2) {
//				if(client != null && client.isRunning()){
//					StringBuilder msg = new StringBuilder();
//					for(int i = 1; i < tokens.length; i++) {
//						msg.append(tokens[i]);
//						if (i != tokens.length -1 ) {
//							msg.append(" ");
//						}
//					}	
//					sendMessage(msg.toString());
//				} else {
//					printError("Not connected!");
//				}
//			} else {
//				printError("No message passed!");
//			}
//		}
		
		else if(tokens[0].equals("disconnect")) {
			disconnect();
			
		} else if (tokens[0].equals("put")) {

			System.out.println("\n[debug]m1-client put: tokens size: " + tokens.length);
			if (tokens.length == 3) {
				//
				if (client != null && client.isRunning()) {
					StringBuilder key = new StringBuilder();
					StringBuilder value = new StringBuilder();
					key.append(tokens[1]);
					value.append(tokens[2]);
					
					CommMessage cm = new CommMessage(StatusType.PUT, OptCode.PUT, key.toString(), value.toString());
					sendCommMessage(cm);
				} else {
					printError("Not connected!");
				}
			} else {
				printError("Incorrect num of arguments; Must be passing in as: put <key> <value> pairs");
			}
			
		} else if (tokens[0].equals("get")) {
			if (tokens.length == 2) {
				if (client != null && client.isRunning()) {
					StringBuilder key = new StringBuilder();
					key.append(tokens[1]);
					
					//FIXME: may be incorrect to set the value to null
					CommMessage cm = new CommMessage(StatusType.GET, OptCode.GET, key.toString(), null);
					sendCommMessage(cm);
				}
			} else {
				printError("Incorrect num of arguments; Must be passing in as: get <key>");
			}
			
		} else if(tokens[0].equals("logLevel")) {
			if(tokens.length == 2) {
				String level = setLevel(tokens[1]);
				if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
					printError("No valid log level!");
					printPossibleLogLevels();
				} else {
					System.out.println(PROMPT + 
							"Log level changed to level " + level);
				}
			} else {
				printError("Invalid number of parameters!");
			}
			
		} else if(tokens[0].equals("help")) {
			printHelp();
		} else {
			printError("Unknown command");
			printHelp();
		}
	}
	
	
	private void sendCommMessage(CommMessage msg) {
		try {
			client.sendCommMessage(msg);
		} catch(IOException e) {
			//TODO: set the status code
			printError("Command failed!");
			disconnect();
		}
	}

	private void connect(String address, int port) 
			throws UnknownHostException, IOException {
		client = new Client(address, port);
		client.addListener(this);
		client.start();
	}
	
	private void disconnect() {
		if(client != null) {
			client.closeConnection();
			client = null;
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
		System.out.println(PROMPT 
				+ "Possible log levels are:");
		System.out.println(PROMPT 
				+ "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
	}

	private String setLevel(String levelString) {
		
		if(levelString.equals(Level.ALL.toString())) {
			logger.setLevel(Level.ALL);
			return Level.ALL.toString();
		} else if(levelString.equals(Level.DEBUG.toString())) {
			logger.setLevel(Level.DEBUG);
			return Level.DEBUG.toString();
		} else if(levelString.equals(Level.INFO.toString())) {
			logger.setLevel(Level.INFO);
			return Level.INFO.toString();
		} else if(levelString.equals(Level.WARN.toString())) {
			logger.setLevel(Level.WARN);
			return Level.WARN.toString();
		} else if(levelString.equals(Level.ERROR.toString())) {
			logger.setLevel(Level.ERROR);
			return Level.ERROR.toString();
		} else if(levelString.equals(Level.FATAL.toString())) {
			logger.setLevel(Level.FATAL);
			return Level.FATAL.toString();
		} else if(levelString.equals(Level.OFF.toString())) {
			logger.setLevel(Level.OFF);
			return Level.OFF.toString();
		} else {
			return LogSetup.UNKNOWN_LEVEL;
		}
	}
	
	@Override
	public void handleNewCommMessage(CommMessage cm) {
		// TODO Auto-generated method stub
		if(!stop) {
			System.out.println(cm.toString());
			System.out.print(PROMPT);
		}
		
	}
	
	@Override
	public void handleStatus(SocketStatus status) {
		if(status == SocketStatus.CONNECTED) {

		} else if (status == SocketStatus.DISCONNECTED) {
			System.out.print(PROMPT);
			System.out.println("Connection terminated: " 
					+ serverAddress + " / " + serverPort);
			
		} else if (status == SocketStatus.CONNECTION_LOST) {
			System.out.println("Connection lost: " 
					+ serverAddress + " / " + serverPort);
			System.out.print(PROMPT);
		}
		
	}

	private void printError(String error){
		System.out.println(PROMPT + "Error! " +  error);
	}
	
    /**
     * Main entry point for the echo server application. 
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
    	try {
			new LogSetup("logs/client.log", Level.OFF);
			KVClient app = new KVClient();
			app.run();
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}
    }
    
    
}
