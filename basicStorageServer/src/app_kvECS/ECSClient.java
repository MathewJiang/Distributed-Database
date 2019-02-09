package app_kvECS;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Collection;
import java.util.Properties;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

import shared.messages.CommMessage;
import client.KVCommInterface;
import client.KVStore;

import app_kvClient.KVClient;
import app_kvClient.ClientSocketListener.SocketStatus;
import app_kvServer.KVServer;

import ecs.IECSNode;

public class ECSClient implements IECSClient {

    @Override
    public boolean start() {
        info("start()");
        warn("start() not implemented");
        return false;
    }

    @Override
    public boolean stop() {
        // TODO
        return false;
    }

    @Override
    public boolean shutdown() {
        // TODO
        return false;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
    	info("addNode(cacheStrategy=" + cacheStrategy + ",cacheSize=" + cacheSize + ")");
        warn("Not implemented yet");
        return null;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        info("addNodes(count=" + count + ", cacheStrategy=" + cacheStrategy + ",cacheSize=" + cacheSize + ")");
        warn("Not implemented yet");
        return null;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        // TODO
        return false;
    }
    public boolean removeNode(int index) {
    	info("removeNode(index=" + index+")");
        warn("no implementation");
        return false;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        // TODO
        return null;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        // TODO
        return null;
    }
    
    public void listNode() {
    	warn("no implementation");
    }

    public static void main(String[] args) {
    	// Hack shared ConnectionUtil interrupt between server and client
		// code.
		KVServer.serverOn = true;
		/*try {
			setUpClientLogger();
		} catch (Exception e) {
			System.out
					.println("Unable to read from resources/config/client-log4j.properties");
			System.out.println("Using default logger from skeleton code.");
			new LogSetup("logs/client-default.log", Level.ALL);
		}*/
		ECSClient app = new ECSClient();
		app.run();
    }
    private static Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "ecs_shell> ";
	private BufferedReader stdin;
	private boolean stop = false;

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
	private void warn(String line) {
		System.out.println("Warning: " + line);
	}
	private void info(String line) {
		System.out.println("Info: " + line);
	}
	private void handleCommand(String cmdLine) {
		String[] tokens = cmdLine.split("\\s+");

		switch (tokens[0]) {

		case "quit":
			stop = true;
			System.out.println(PROMPT + "Application exit!");
			break;

		case "addNodes":
			if (tokens.length == 4) {
				addNodes(Integer.parseInt(tokens[1]), tokens[3], Integer.parseInt(tokens[2]));
			} else {
				warn("Usage addNodes <numberOfNodes> <cacheSize> <replacementStrategy>");
			}
			break;
		case "start":
			if (tokens.length != 1) {
				warn("start does not have any arguments");
			} else {
				start();
			}
			break;
		case "stop":
			if (tokens.length != 1) {
				warn("end does not have any arguments");
			} else {
				stop();
			}
			break;
		case "shutDown":
			if (tokens.length != 1) {
				warn("shutDown does not have any arguments");
			} else {
				shutdown();
			}
			break;
		case "addNode":
			if (tokens.length == 3) {
				addNode(tokens[3], Integer.parseInt(tokens[2]));
			} else {
				warn("Usage addNode <cacheSize> <replacementStrategy>");
			}
			break;
		case "removeNode":
			if (tokens.length == 2) {
				removeNode(Integer.parseInt(tokens[1]));
			} else {
				warn("Usage removeNode <index>");
			}
			break;
		case "listNode":
			if (tokens.length !=1) {
				warn("listNode does not have any argument");
			} else {
				listNode();
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

	public void handleNewCommMessage(CommMessage cm) {
		if (!stop) {
			System.out.println(cm.toString());
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

}
