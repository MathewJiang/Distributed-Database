package app_kvECS;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import shared.ConsistentHash;
import shared.MD5;
import shared.messages.CommMessage;
import shared.metadata.InfraMetadata;
import shared.metadata.ServiceLocation;
import app_kvServer.KVServer;
import ecs.ECSNode;
import ecs.IECSNode;

public class ECSClient implements IECSClient {

	private ECS ecs;
	private final String ecs_config = "./resources/config/ecs.config";
	private InfraMetadata MD = new InfraMetadata();
	List<ServiceLocation> launchedServer = new ArrayList<ServiceLocation>();
	List<IECSNode> launchedNodes = new ArrayList<IECSNode>();
	List<ServiceLocation> avaliableSlots = new ArrayList<ServiceLocation>();
	private ConsistentHash hashRing;
	private String currDir = "/";
	private Set<String> SetDir = new HashSet<String>();
	private int ECSport = 39678;
    private static Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "ecs_shell>";
	private BufferedReader stdin;
	private boolean stop = false;
	private String workDir = "";
	private String config = "";
	private String ecsLocation = "127.0.0.1";
	
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
	
	public ECS getECS(){
		return ecs;
	}
	
	// set start = true for all client server
    @Override
    public boolean start() {
    	ecs.broadast("START");
    	return true;
    }

    @Override
    public boolean stop() {
    	ecs.broadast("STOP");
    	return true;
    }

    @Override
    public boolean shutdown() {
       	// pick a node
    	List<ServiceLocation> servers = ecs.getMD().getServerLocations();
    	if(servers.size() == 0) {
    		warn("trying to shutdown but there is no server");
    		return true;
    	}
		Collections.shuffle(servers, new Random(servers.size()));
		echo("Shutdown " + servers.size() + " servers");
		// pick the first as leader
		ServiceLocation leader = servers.get(0);
		ecs.setLeader(leader.serviceName, leader.host);
		ecs.waitAckSetup("terminate");
    	ecs.broadast("SHUTDOWN");
    	ecs.waitAck("terminate", launchedNodes.size(), 50);
    	ecs.ack("ECSclient", "backupCompleted");
		ecs.waitAckSetup("restoreCompleted");
    	ecs.waitAck("restoreCompleted", 1, 50);
    	launchedNodes.clear();
    	launchedServer.clear();
    	ecs.setConfigured(false);
    	//ecs.reset();
    	
    	ecs.deleteHeadRecursive("/nodes");
    	ecs.deleteHeadRecursive("/lock");
    	ecs.deleteHeadRecursive("/ack");
    	ecs.init();
        return true;
    }

    private String getNewServerName() {
    	List<String> serverList = ecs.returnDirList("/nodes");
    	serverList.size();
    	String alias = "server_";
    	int[] serverNumbers = new int[serverList.size()];
    	for(int i = 0; i < serverList.size(); i++) {
    		serverNumbers[i] = Integer.parseInt(serverList.get(i).substring(7));
    	}
    	Arrays.sort(serverNumbers);
    	for(int i = 0; i < serverList.size(); i++) {
    		
    		if(serverNumbers[i] != i) {
    			String curr = alias + i;
        		echo("curr is " + curr);
    			// inconsistency
    			return curr;
    		}
    	}
    	int serial = launchedNodes.size();
    	String name = "server_" + serial;
    	return name;
    }
    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
    	info("addNode(cacheStrategy=" + cacheStrategy + ",cacheSize=" + cacheSize + ")");
    	
    	hashRing.removeAllServerNodes();
    	String name = getNewServerName();
    	
    	ServiceLocation spot;
    	spot = avaliableSlots.get(0);
		avaliableSlots.remove(0);
		
    	spot.serviceName = name;
    	
    	// hash
    	InfraMetadata new_MD = ecs.getMD();
    	List<ServiceLocation> tmp = new_MD.getServerLocations();
    	tmp.add(spot);
    	new_MD.setServerLocations(tmp);
    	hashRing.addNodesFromInfraMD(new_MD);
    	try {
			IECSNode newNode = new ECSNode(name, spot.host, spot.port, hashRing.getHashRange(spot));
			launchedNodes.add(newNode);
			// launchedServer not handled
			// incrementally update MD
			
			ecs.lock();
			
			
			//ecs.broadast("UPDATE");
			// we changed to use unit cast to affected node
			
			String affectedServerName = hashRing.getSuccessor(spot).serviceName;
			ecs.setCmd(affectedServerName, "LOCK_WRITE");
			
			
			ecs.addOneLaunchedNodes(newNode);
			ecs.refreshHash(hashRing);
			ecs.waitAckSetup("launched");
			launch(newNode.getNodeHost(), newNode.getNodeName(), ECSport, cacheStrategy, cacheSize);
			ecs.waitAck("launched", 1, 50); // new node launched
			ecs.waitAckSetup("migrate");
			ecs.unlock(); // allow effected node to migrate
			ecs.waitAck("migrate", 1, 50); // internal unlock -> new nodes migrated
			ecs.waitAckSetup("sync");
			ecs.broadast("SYNC"); // Including launched new server
			ecs.waitAck("sync", launchedNodes.size(), 50); 
			return newNode;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return null;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        info("addNodes(count=" + count + ", cacheStrategy=" + cacheStrategy + ",cacheSize=" + cacheSize + ")");
        for(int i = 0; i < count; i++) {
        	addNode(cacheStrategy, cacheSize);
        }
        return null;
    }
    
    private void loadECSconfigFromFile() {
		try {
			MD = InfraMetadata.fromConfigFile(ecs_config);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

    private void launch(String remoteIP, String serverName, int ECSport, String strategy, int cache_size) {
    	Runtime run = Runtime.getRuntime();
    	Process proc;
    	info("launch(ip = " + remoteIP + " port = " + ECSport + ")");
    	if(remoteIP.equals("127.0.0.1") || remoteIP.equals("localhost")) {
    		try {
				proc = Runtime.getRuntime().exec(nossh_launch_array(serverName, ECSport, strategy, cache_size));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	} else {
    		
    		try {
    			proc = run.exec(ssh_launch_array(remoteIP, serverName, ECSport, strategy, cache_size));
    		} catch (IOException e) {
    			e.printStackTrace();
    		}
    	}
    }
    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
    	if(ecs.configured()) {
    		return ecs.getECSNodeCollection();
    	}
    	
    	if(ecs.inited()) {
    		ecs.deleteHeadRecursive("/nodes");
    		ecs.create("/nodes", null, "-p");
    	}
    	
    	
    	
    	ecs.waitAckSetup("launched");
    	loadECSconfigFromFile();
    	hashRing.removeAllServerNodes();
		List<ServiceLocation> servers = MD.getServerLocations();
		Collections.shuffle(servers, new Random(count)); 
		info("Launching " + count + "/" + servers.size() + " servers on file");
		// need to construct a finished MD
		InfraMetadata new_MD = new InfraMetadata();
		List<ServiceLocation> selectedServers = new ArrayList<ServiceLocation>();
		for(int i = 0; i < count; i++) {
			ServiceLocation curr = servers.get(i);
			selectedServers.add(curr);
		}
		for(int i = count; i < servers.size(); i++) {
			avaliableSlots.add(servers.get(i));
		}
		new_MD.setServerLocations(selectedServers);
		hashRing.addNodesFromInfraMD(new_MD);
		for(int i = 0; i < count; i++) {
			ServiceLocation curr = servers.get(i);
			launchedServer.add(curr);
			ECSNode item_to_be_added;
			try {
				item_to_be_added = new ECSNode(curr.serviceName, curr.host, curr.port, hashRing.getHashRange(curr));
				launchedNodes.add(item_to_be_added);	
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		info("About to launch " + launchedServer.size() + " servers cacheStrategy " + cacheStrategy + " cacheSize " + cacheSize);
        List<IECSNode> aliased = ecs.setLaunchedNodes(launchedNodes);
        for(int i = 0; i < aliased.size(); i++) {
        	IECSNode curr = aliased.get(i);
        	echo("Launching " + curr.getNodeName());
        	launch(curr.getNodeHost(), curr.getNodeName(), ECSport, cacheStrategy, cacheSize);
        }
        ecs.waitAck("launched", count, 50);
        
        if(ecs.existsLeader()){
    		echo("leader exists, trying to restore the saved data");
    		ecs.waitAckSetup("backupRestored");
    		launch("127.0.0.1", "restore_server", 0, "LRU", 200);
    		ecs.waitAck("backupRestored", 1, 50);
    		
    	} else {
    		echo("fresh startup");
    	}
        
		return aliased;
    }
    

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
    	ecs.waitAckSetup("report");
    	ecs.broadast("REPORT");
    	ecs.waitAck("report", count, timeout);
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        // check if name exists
    	InfraMetadata currMD = ecs.getMD();
    	List<String> nodeNamesList = (List<String>) nodeNames;
    	List<ServiceLocation> nodesFromMD = currMD.getServerLocations();
    	for(int j = 0; j < nodeNames.size(); j++) {
    		for(int i = 0; i < nodesFromMD.size(); i++) {
    			if(nodesFromMD.get(i).serviceName.equals(nodeNamesList.get(j))) {
    				// we found one match
    				removeNode(nodeNamesList.get(j));
    			}
    		}
    	}
    	return false;
    }
    
    private ServiceLocation getReturnedSlot(String name) {
    	for(int i = 0; i < launchedNodes.size(); i++) {
    		if(launchedNodes.get(i).getNodeName().equals(name)) {
    			IECSNode toBeRemoved = launchedNodes.get(i);
    			launchedNodes.remove(i);
    			ServiceLocation toBeRemovedSL = new ServiceLocation(toBeRemoved.getNodeName(), toBeRemoved.getNodeHost(), toBeRemoved.getNodePort());
    			return toBeRemovedSL;
    		}
    	}
    	return null;
    }
    
    private int indexServiceLocation(List<ServiceLocation> target, String name) {
    	for(int i = 0; i < target.size(); i++) {
    		if(target.get(i).serviceName.equals(name)) {
    			return i;
    		}
    	}
    	return -1;
    }
    public boolean removeNode(String name) {
    	info("removeNode(name = " + name + ")");
    	
    	ServiceLocation returnedSlot = getReturnedSlot(name);
    	
    	InfraMetadata new_MD = ecs.getMD();
    	List<ServiceLocation> tmp = new_MD.getServerLocations();
    	int deleteIndex = indexServiceLocation(tmp, returnedSlot.serviceName);
    	if(deleteIndex == -1) {
    		warn("delete node not found");
    		launchedNodes.add((IECSNode) returnedSlot); // recover
    		return false;
    	}
    	ConsistentHash oldHash = new ConsistentHash();
    	oldHash.addNodesFromInfraMD(new_MD);
    	
    	hashRing.removeAllServerNodes();
    	tmp.remove(deleteIndex);
    	new_MD.setServerLocations(tmp);
    	hashRing.addNodesFromInfraMD(new_MD);
    
    	ecs.lock();
		
		String affectedServerName;
		try {
			affectedServerName = oldHash.getSuccessor(returnedSlot).serviceName;
			
			ecs.waitAckSetup("computedNewMD");
			ecs.setCmd(affectedServerName, "LOCK_WRITE_REMOVE_RECEVIER");
			ecs.waitAck("computedNewMD", 1, 50);
			ecs.setCmd(returnedSlot.serviceName, "LOCK_WRITE_REMOVE_SENDER");
			
			ecs.refreshHash(hashRing);
			ecs.waitAckSetup("migrate");
			ecs.unlock();
			ecs.waitAck("migrate", 1, 50); // internal unlock
			ecs.waitAckSetup("sync");
			
			ecs.deleteHeadRecursive("/nodes/" + returnedSlot.serviceName);
			ecs.broadast("SYNC");
			ecs.waitAck("sync", launchedNodes.size(), 50);
			
			
	    	avaliableSlots.add(returnedSlot);
	    	return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return false;
		
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        // TODO
        return null;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
    	String MD5Key = MD5.getMD5String(Key);
    	echo("Key hashed to " + MD5Key);
    	
    	// refresh hashRing
    	hashRing.removeAllServerNodes();
    	InfraMetadata new_MD = ecs.getMD();
    	hashRing.addNodesFromInfraMD(new_MD);
    	
    	
        ServiceLocation server = hashRing.getServer(Key);
        echo("HashRing gives " + server.serviceName);
        for(int i = 0; i < launchedNodes.size(); i++) {
        	echo(launchedNodes.get(i).getNodeName());        	
        	if(launchedNodes.get(i).getNodeName().equals(server.serviceName)) {
        		echo("Key hashed to server " + launchedNodes.get(i).getNodeName());
        		return launchedNodes.get(i);
        	}
        }
        return null;
    }
    
    public void listNode() {
    	warn("no implementation");
    }
    
    private String ssh_launch(String remoteIP, String serverName, int ECSport, String strategy, int cache_size) {
    	echo("ssh -n "+ remoteIP +" nohup \"sh -c \'cd "+ workDir 
    			+ " " + "&& java -jar ./m2-server.jar " + ECSport + " "+  serverName +" "  + cache_size +" " + strategy + " &" + "\'\"");
    	return "ssh -n "+ remoteIP +" nohup \"sh -c \'cd "+ workDir 
    			+ " " + "&& java -jar ./m2-server.jar " + ECSport + " "+  serverName +" "  + cache_size +" " + strategy + " &" + "\'\"";
    }
    
    private String nossh_launch(String serverName, int ECSport, String strategy, int cache_size) {
    	echo("sh -c \'cd "+ workDir 
    			+ " " + "&& java -jar ./m2-server.jar " + ECSport + " "+  serverName +" "  + cache_size +" " + strategy + " &" + "\'");
    	return("sh -c \'cd "+ workDir 
    			+ " " + "&& java -jar ./m2-server.jar " + ECSport + " "+  serverName +" "  + cache_size +" " + strategy + " &" + "\'");
    }
    
    private String[] ssh_launch_array(String remoteIP, String serverName, int ECSport, String strategy, int cache_size) {
    	String[] cmd = new String[3];
    	cmd[0] = "ssh";
    	cmd[1] = "-n";
    	cmd[2] = remoteIP +" nohup \"sh -c \'cd "+ workDir 
    			+ " " + "&& java -jar ./m2-server.jar " + ECSport + " "+  serverName +" "  + cache_size +" " + strategy + " &" + "\'\"";
    	return cmd;
    }
    private String[] nossh_launch_array(String serverName, int ECSport, String strategy, int cache_size) {
    	String[] cmd = new String[3];
    	cmd[0] = "sh";
    	cmd[1] = "-c";
    	cmd[2] = "cd "+ workDir 
    			+ " " + "&& java -jar ./m2-server.jar " + ECSport + " "+  serverName +" "  + cache_size +" " + strategy + " &";
    	echo(cmd[0]);
    	echo(cmd[1]);
    	echo(cmd[2]);
    	return cmd;
    }
    
	private void set_workDir () {
		workDir = System.getProperty("user.dir");
		info("work directory is: " + workDir);
	}

    /**
     * run():
     * execute the command shell
     */
	public void run() {
		ecs = new ECS();
		set_workDir();
		hashRing = new ConsistentHash();
		
		
		try {
			ecs.connect(ecsLocation, 39678);
			if(ecs.configured()) {
	    		restoreFromECS();
	    	}
			
			
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
	
	public void initECS() {
		ecs = new ECS();
		set_workDir();
		hashRing = new ConsistentHash();
		
		
		try {
			ecs.connect(ecsLocation, 39678);
			if(ecs.configured()) {
	    		restoreFromECS();
	    	}
			
			
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	private void restoreFromECS() {
		MD = ecs.getMD();
		launchedServer = MD.getServerLocations();
		launchedNodes = new ArrayList<IECSNode>();
		// restore hashRing
		hashRing.removeAllServerNodes();
		hashRing.addNodesFromInfraMD(MD);
		try {
			InfraMetadata tmpMD = InfraMetadata.fromConfigFile(ecs_config);
			List<ServiceLocation> allSlots = tmpMD.getServerLocations();
			avaliableSlots = new ArrayList<ServiceLocation>();
			for(int i = 0; i < launchedServer.size(); i++) {
				for(int j = 0; j < allSlots.size(); j++) {
					ServiceLocation curr = launchedServer.get(i);
					if(curr.serviceName.equals(allSlots.get(j).serviceName)) {
						// we have a match so this is not going to available slots
						IECSNode newNode = new ECSNode(curr.serviceName, curr.host, curr.port, hashRing.getHashRange(curr));
						launchedNodes.add(newNode);
					} else {
						avaliableSlots.add(curr);
					}
				}
			}
			echo("restore success");
			return;
		} catch (Exception e) {
			e.printStackTrace();
		}
		echo("restore failed");
		
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
		System.out.println("[ECSClient.java]Warning: " + line);
		logger.error("[ECSClient.java]: " + line);
	}
	
	private void info(String line) {
		//System.out.println("[ECSClient.java]Info: " + line);
		logger.info("[ECSClient.java]Info: " + line);
	}

	private void echo(String line) {
		//System.out.println("[ECSClient.java]: " + line);
		logger.info("[ECSClient.java]: " + line);
	}

	private void handleCommand(String cmdLine) {
		String[] tokens = cmdLine.split("\\s+");

		switch (tokens[0]) {
		case "ecsOverride":
			if(tokens.length == 1) { 
				ecsLocation = tokens[0];
			}
			break;
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
			if (tokens.length != 1 && tokens.length != 2) {
				warn("start does not have any arguments");
			} else {
				
				start();
			}
			break;
			
		case "stop":
			// FIXME: remove the second condition
			if (tokens.length != 1 && tokens.length != 2) {
				warn("stop does not have any arguments");
			} else {
				stop();
			}
			break;
			
		case "shutDown":
			shutdown();
			break;
			
		case "addNode":
			if (tokens.length == 3) {
				addNode(tokens[2], Integer.parseInt(tokens[1]));
			} else {
				warn("Usage addNode <cacheSize> <replacementStrategy>");
			}
			break;
			
		case "removeNodes":
			if (tokens.length >= 2) {
				List<String> serverNames = new ArrayList<String>();
				for(int i = 1; i < tokens.length; i++) {
					serverNames.add(tokens[i]);
				}
				removeNodes(serverNames);
			} else {
				warn("Usage removeNodes <string>[]");
			}
			break;
			
		case "listNode":
			if (tokens.length !=1) {
				warn("listNode does not have any argument");
			} else {
				listNode();
			}
			break;
		case "getNodeByKey":
			if(tokens.length == 2) {
				IECSNode result = getNodeByKey(tokens[1]);
				if(result != null) {
					echo(tokens[1] + " -> " + result.getNodeName());
				} else {
					warn("getNodeByKey returned null");
				}
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
				} else if(tokens[1].equals("/")) { 
					currDir = "/";
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
					String safeCurrDir = currDir;
					if(!currDir.endsWith("/")) {
						safeCurrDir += "/";
					}
					ecs.create(safeCurrDir +tokens[i], emptyByte, tokens[1]);
				}
			}
			break;
			
		case "rm":
			if(tokens.length == 1) {
				break;
			} else if(tokens.length >= 2) {
				if(!currDir.endsWith("/") && !tokens[1].equals("-r")) {
					if(!ecs.returnDirSet(currDir).contains(tokens[1])) {
						echo("rm: cannot remove \'"+tokens[1]+"\': No such file or directory");
						break;
					}
				}
				String safeCurrDir = currDir;
				if(!currDir.endsWith("/")) {
					safeCurrDir += "/";
				}
				if(tokens[1].equals("-r")) {
					if(!ecs.returnDirSet(currDir).contains(tokens[2])) {
						echo("rm: cannot remove \'"+tokens[2]+"\': No such file or directory");
						break;
					}
					if(tokens[1].startsWith("/")) {
						ecs.deleteHeadRecursive(tokens[2]);
					} else {
						ecs.deleteHeadRecursive(safeCurrDir+tokens[2]);
					}
				} else {
					if(tokens[1].startsWith("/")) {
						ecs.deleteHead(tokens[1]);
					} else {
						if(ecs.returnDirSet(currDir + tokens[1]).size() !=0) {
							echo("rm: cannot remove \'" + tokens[1] + "\': Is a directory");
							break;
						}loadECSconfigFromFile();
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
						ecs.setData(currDir + tokens[3], tokens[1]);
					}
				}
			}
			break;
		case "cat":
			if(tokens.length == 2) {
				String safeCurrDir = currDir;
				if(!currDir.endsWith("/")) {
					safeCurrDir += "/";
				}
				echo(ecs.getData(safeCurrDir + tokens[1]));
			}
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
		case "printMD":
			if (tokens.length == 1) {
				ecs.printMD();
			}
			break;
		case "reset":
			if (tokens.length == 2) {
				if(tokens[1].equals("-all")) {
					ecs.broadast("SHUTDOWN");
					launchedNodes.clear();
					launchedServer.clear();
					ecs.reset();
					ecs.init();
					ecsLocation = "127.0.0.1";
				}
			}
			break;
		case "printHash":
			if (tokens.length == 1) {
				ecs.printHash();
			}
			break;
		case "lock":
			if (tokens.length == 1) {
				ecs.lock();
			}
			break;
		case "unlock": 
			if (tokens.length == 1) {
				ecs.unlock();
			}
			break;
		case "getcmd":
			if (tokens.length == 2) {
				echo(ecs.KVAdminMessageTypeToString(ecs.getCmd(tokens[1]).getKVAdminMessageType()));
			}
			break;
		case "setcmd":
			if(tokens.length == 3) {
				ecs.setCmd(tokens[1], tokens[2]);
			}
			break;
		case "ack":
			if (tokens.length == 3) {
				ecs.ack(tokens[1], tokens[2]);
			}
			break;
		case "waitAck":
			if (tokens.length == 4) {
				ecs.waitAckSetup(tokens[1]);
				ecs.waitAck(tokens[1], Integer.parseInt(tokens[2]), Integer.parseInt(tokens[3]));
			}
			break;
		// testing purposes only
		case "migrate":
			ecs.broadast("UPDATE");
			break;
		case "locktest":
			for(int i = 0; i < 2000; i++) {
				ecs.lock();
				ecs.unlock();
				echo("lock(" + i + ")");
			}
			echo("done");
			break;
		case "awaitNodes":
			if(tokens.length == 3) {
				try {
					awaitNodes(Integer.parseInt(tokens[1]), Integer.parseInt(tokens[2]));
				} catch (Exception e) {
					e.printStackTrace();
				}
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

	@SuppressWarnings("unused")
	private void printHelp() {
		
		
		/*StringBuilder sb = new StringBuilder();
		sb.append(PROMPT).append("M2 CLIENT HELP (Usage):\n");
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

		sb.append("logLevel");;
		sb.append("\t\t\t changes the logLevel. Available levels: {ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF}\n");

		sb.append("quit ");
		sb.append("\t\t\t\t exits the program");
		sb.append("\n************************************************************************************\n");
		*/
		System.out.println("ack\naddNode <cache size> <policy>" +
				"\naddNodes <num nodes> <cacheSize> <policy>\nawaitNodes" +
				" <count> <timeout>\ncat <file> : read data out\ncd <dir>" +
				"\ncreate -p/-s <znode name[]>\necho <content> > <znode> :" +
				" write data to znode\nexit : exit the shell\ngetNodeByKey " +
				"<key> : return which server it goes\ngetcmd <serverName> :" +
				" get cmd for server by name\nlock : lock globalLock\nlocktest :" +
				" testcmd, will run 2000 lock and unlock ops\nlogLevel : adjust log " +
				"level\nls : list dir\nmigrate : ask every server to update\nprintHash " +
				": print the hash map stored\nprintMD : print the metadata stored\nquit :" +
				"exit the shell\nremoveNodes <string[] of servers>\nreset -all: " +
				"reset everything\nrm <znode>, remove znode, if this znode has" +
				" children use -r\nsetcmd <serverName> <command>\nsetupNodes <num nodes>" +
				" <policy> <cache size>\nshutDown no argument: shutdown all servers\nstart " +
				"-- no argument: start all servers\nstop -- no argument:" +
				" stop all servers\nunlock -- no argument: " +
				"unlock globalLock\nwaitAck -- <event> <num ack required> <timeout>:" +
				" setup an event for others to ack\n\n");
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

	private static void setUpESCClientLogger() throws Exception {
		Properties props = new Properties();
		props.load(new FileInputStream("resources/config/server-log4j.properties"));
		props.put("log4j.appender.fileLog.File", "logs/ecs-default-log.out");
		PropertyConfigurator.configure(props);
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
		try {
			setUpESCClientLogger();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		app.run();
    }
    
}
