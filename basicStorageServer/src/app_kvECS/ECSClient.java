package app_kvECS;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Collection;
import java.util.Properties;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

import shared.InfraMetadata;
import shared.InfraMetadata.ServiceLocation;
import shared.messages.CommMessage;
import client.KVCommInterface;
import client.KVStore;

import app_kvClient.KVClient;
import app_kvClient.ClientSocketListener.SocketStatus;
import app_kvServer.KVServer;

import ecs.ECSNode;
import ecs.IECSNode;
import app_kvECS.ECS;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class ECSClient implements IECSClient {

	private ECS ecs;
	private final String ecs_config = "./resources/config/ecs.config";
	private InfraMetadata MD = new InfraMetadata();
	List<ServiceLocation> launchedServer = new ArrayList<ServiceLocation>();
	List<ECSNode> launchedNodes = new ArrayList<ECSNode>();
	private String currDir = "/";
	private Set<String> SetDir = new HashSet<String>();
	//https://stackoverflow.com/questions/11208479/how-do-i-initialize-a-byte-array-in-java
	public static byte[] hexStringToByteArray(String s) {
	    int len = s.length();
	    byte[] data = new byte[len / 2];
	    for (int i = 0; i < len; i += 2) {
	        data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
	                             + Character.digit(s.charAt(i+1), 16));
	    }
	    return data;
	}
	
	// set start = true for all client server
    @Override
    public boolean start() {
        info("start()");
        warn("start() implementation in progress");
        byte[] test = hexStringToByteArray("e04fd020ea3a6910a2d808002b30309d");
        /*try {
			zk.create("/node1", test);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
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
    
    private void loadECSconfig() {
		try {
			MD = InfraMetadata.fromConfigFile(ecs_config);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

    private void launch(String ip, int port) {
    	Runtime run = Runtime.getRuntime();
    	Process proc;
    	info("launch(ip = " + ip + " port = " + port + ")");
    	if(ip.equals("127.0.0.1") || ip.equals("localhost")) {
    		try {
				proc = run.exec(nossh_launch(ip, port));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	} else {
    		
    		try {
    			proc = run.exec(ssh_launch(ip, port));
    		} catch (IOExceptionpath e) {
    			e.printStackTrace();
    		}
    	}
    }
    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
    	loadECSconfig();
		List<ServiceLocation> servers = MD.getServerLocations();
		Collections.shuffle(servers, new Random(count)); 
		info("Launching " + count + "/" + servers.size() + " servers on file");
		for(int i = 0; i < count; i++) {
			ServiceLocation curr = servers.get(i);
			launchedServer.add(curr);
			ECSNode item_to_be_added = new ECSNode(curr.serviceName, curr.host, curr.port, "FFF", "000");
			launch(curr.host, curr.port);
			launchedNodes.add(item_to_be_added); // fake values
		}
		info("launched " + launchedServer.size() + " servers cacheStrategy " + cacheStrategy + " cacheSize " + cacheSize);
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
    
    private String ssh_launch(String ip, int port) {
    	return "ssh -n "+ ip +" nohup \"sh -c \'cd "+ workDir 
    			+ " " + "&& java -jar ./m2-server.jar &" + port + "\'\"";
    }
    
    private String nossh_launch(String ip, int port) {
    	return "sh -c \'cd "+ workDir 
    			+ " " + "&& java -jar ./m2-server.jar &" + port + "\'\"";
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
	private static final String PROMPT = "ecs_shell";
	private BufferedReader stdin;
	private boolean stop = false;
	private String workDir = "";
	private String config = "";
	
	private void set_workDir () {
		workDir = System.getProperty("user.dir");
		info("work directory is: " + workDir);
	}
	public void run() {
		ecs = new ECS();
		set_workDir();
		try {
			ecs.connect("127.0.0.1", 40000);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		while (!stop) {
			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(PROMPT+":"+currDir + "$ ");

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
	private void echo(String line) {
		System.out.println(line);
	}
	private void handleCommand(String cmdLine) {
		String[] tokens = cmdLine.split("\\s+");

		switch (tokens[0]) {
		
		case "status":
			printStatus();
			break;

		case "quit":
			stop = true;
			System.out.println(PROMPT + "Application exit!");
			break;
		case "exit":
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
		case "":
			break;
		case "setupNodes":
			if (tokens.length == 4) {
				setupNodes(Integer.parseInt(tokens[1]), tokens[2], Integer.parseInt(tokens[3]));
			} else {
				warn("Usage setupNodes <count> <String Strategy> <cacheSize>");
			}
			break;
		case "ls":
			if (tokens.length == 1) {
				ecs.printPath(currDir);
			} else if(tokens.length == 2) {
				ecs.printPath(currDir + tokens[1]);
			} else {
				warn("ls should take 1 or no args");
			}
			break;
		case "cd":
			if (tokens.length == 1) {
				currDir = "/";
			} else if(tokens.length == 2) {
				if(ecs.returnDirSet(currDir).contains(tokens[1])) {
					if(currDir != "/") {
						currDir += "/";
					}
					currDir +=tokens[1];
				} else if(tokens[1].equals(".")) {
					break;
				} else if(tokens[1].equals("..")) {
					if(currDir.equals("/")) {
						break;
					}
					String[] parts = currDir.split("/");
					if(parts.length > 2) {
					currDir = "";
						for(int i = 0; i < parts.length - 1; i++) {
							currDir += parts[i] + "/";
						}
						if(currDir.equals("/")) {
							break;
						}
						currDir = currDir.substring(0, currDir.length() -1);
					} else {
						currDir = "/";
					}
				} else {
				
					echo("ecs_shell cd: " + tokens[1] + " : No such file or directory");
				}
			} else {
				warn("cd should take 1 or no args");
			}
			break;
		case "create":
			if(tokens.length == 1) {
				break;
			} else if(tokens.length >= 3) {
				for(int i = 2; i < tokens.length;i++) {
					byte[] emptyByte = null;
					try {
						String safeCurrDir = currDir;
						if(!currDir.endsWith("/")) {
							safeCurrDir += "/";
						}
						ecs.create(safeCurrDir +tokens[i], emptyByte, tokens[1]);
					} catch (KeeperException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			break;
		case "rm":
			if(tokens.length == 1) {
				break;
			} else if(tokens.length == 2) {
				if(!ecs.returnDirSet(currDir).contains(tokens[1])) {
					echo("rm: cannot remove \'"+tokens[1]+"\': No such file or directory");
					break;
				}
				String safeCurrDir = currDir;
				if(!currDir.endsWith("/")) {
					safeCurrDir += "/";
				}
				if(ecs.returnDirSet(currDir + tokens[1]).size() !=0) {
					echo("rm: cannot remove \'" + tokens[1] + "\': Is a directory");
				} else if(tokens[1].equals("-r")) {
					
					if(tokens[1].startsWith("/")) {
						ecs.deleteHead(tokens[1]);
					} else {
						ecs.deleteHead(safeCurrDir+tokens[1]);
					}
				} else {
					if(tokens[1].startsWith("/")) {
						ecs.deleteHead(tokens[1]);
					} else {
						ecs.deleteHead(safeCurrDir+tokens[1]);
					}
				}
			}
			break;
		case "echo":
			if(tokens.length >= 2) {
				if(tokens.length == 2) {
					echo(tokens[1]);
				}
				if(tokens.length == 4) {
					if(tokens[2].equals(">")) {
						ecs.setData(currDir + tokens[3], tokens[1].getBytes());
					}
				}
			}
			break;
		case "cat":
			if(tokens.length == 2) {
				echo(ecs.getData(currDir + tokens[1]));
			}
			break;
		default:
			printError("Unknown command");
			printHelp();
			return;
		}
	}

	private void printStatus() {
		// TODO Auto-generated method stub
		
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
