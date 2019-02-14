/***************************************************************
 * ECS:
 * 
 * ECS backend implementation
 ***************************************************************/

package app_kvECS;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import shared.InfraMetadata;
import shared.InfraMetadata.ServiceLocation;
import ecs.ECSNode;
import ecs.IECSNode;

public class ECS {

	private ZooKeeper zk;
	CountDownLatch connectionLatch = new CountDownLatch(1);
	
	//String logConfigFileLocation = "./";
	
	/*****************************************************************************
	 * connect:
	 * connect the zk
	 * @param	host	name of the host
	 * @param	port	port number of the host
	 * @return 			connected zk
	 *****************************************************************************/
	public ZooKeeper connect(String host, int port) throws IOException, InterruptedException {
		org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.INFO);
		String connectString = host +":"+port;
		zk = new ZooKeeper(connectString, 500, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				if (event.getState() == KeeperState.SyncConnected) {
                    connectionLatch.countDown();
                }
			}
			
		});
		// registering some lock
		byte[] emptyByte = null;
		try {
			if(zk.exists("/lock",true) == null) {
				create("/lock", emptyByte, "-p");
			}
			if(zk.exists("/lock/spinlock",true) == null) {
				create("/lock/spinlock", emptyByte, "-p");
			}
			
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		connectionLatch.await();
		echo("Connected");
		return zk;
	}
	
	/*****************************************************************************
	 * close:
	 * close the zk
	 *****************************************************************************/
	public void close() throws InterruptedException{
		zk.close();
	}
	
	public String create(String path, byte[] data, String mode){
		CreateMode CMode = CreateMode.EPHEMERAL;
		switch(mode) {
			case"-p":
				CMode = CreateMode.PERSISTENT;
				break;
			case"-ps":
				CMode = CreateMode.EPHEMERAL_SEQUENTIAL;
				break;
			case"-e":
				CMode = CreateMode.EPHEMERAL;
				break;
			case"-es":
				CMode = CreateMode.EPHEMERAL_SEQUENTIAL;
				break;	
		}
		String result = null;
		try {
			result = zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,CMode);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}
	
	public void echo(String line){
		System.out.println(line);
	}
	public void printPath(String path) {
		try {
			List<String> Children = zk.getChildren(path, false);
			Iterator<String> it  = Children.iterator();
			while (it.hasNext()) {
				System.out.println(it.next());
			}
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public Set<String> returnDirSet(String path) {
		Set<String> DirSet = new HashSet<String>();
		try {
			List<String> Children = zk.getChildren(path, false);
			Iterator<String> it  = Children.iterator();
			while (it.hasNext()) {
				String curr = it.next();
				DirSet.add(curr);
			}
			return DirSet;
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return DirSet;
	}
	public List<String> returnDirList(String path) {
		try {
			List<String> Children = zk.getChildren(path, false);
			return Children;
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public void deleteHead(String target) {
		try {
			echo("delete "+target);
			zk.delete(target,zk.exists(target,true).getVersion());
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void setData(String path, String data) {
		byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);
		try {
			zk.setData(path, dataBytes, -1);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public String getData(String path) {
		try {
			String data = new String((zk.getData(path, false, null)),  StandardCharsets.US_ASCII);
			return data;
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (java.lang.NullPointerException e) {
			return "";
		}
		return "";
	}

	public void deleteHeadRecursive(String target) {
		echo("delete " + target);
		List<String> curr_children = returnDirList(target);
		Iterator<String> it  = curr_children.iterator();
		while (it.hasNext()) {
			String curr = it.next();
				if(target.equals("/")) {
					continue;
				}
				if(target.endsWith("/")) {
					target = target.substring(0, target.length() -1);
				}
				if(returnDirList(target).size() != 0) {
					if(!target.endsWith("/")) {
						target += "/";
					}
					deleteHeadRecursive(target + curr);
				}
		}
		if(target.endsWith("/")) {
			target = target.substring(0, target.length() -1);
		}
		if(returnDirList(target).size() == 0) {
			
			try {
				//echo("real delete " + target);
				zk.delete(target,zk.exists(target,true).getVersion());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (KeeperException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public boolean configured() {
		try {
			if(zk.exists("/configureStatus", false) == null) {
				zk.create("/configureStatus", ("false").getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
				return false;
			} else {
				return (getData("/configureStatus").equals("true"));
			}
			
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
	public List<IECSNode> getECSNodeCollection() {
		// echo("Info: get from ECS repo");
		List<String> servers = returnDirList("/nodes");
		List<IECSNode> nodelist = new ArrayList<IECSNode>();
		for(int i = 0; i < servers.size();i++) {
			String serverName = servers.get(i);
			String serverPath = "/nodes/" + serverName + "/";
			String NodeHost = getData(serverPath + "NodeHost"); 
			int NodePort = Integer.parseInt(getData(serverPath + "NodePort")); 
			String from = getData(serverPath + "from"); 
			String to = getData(serverPath + "to");
			String[] range = new String[2];
			range[0] = from;
			range[1] = to;
			nodelist.add(new ECSNode(serverName, NodeHost, NodePort, range));
		}
		return nodelist;
	}
	public InfraMetadata getMD() {
		InfraMetadata MD = new InfraMetadata();
		List<ServiceLocation> serverLocations = new ArrayList<ServiceLocation>();
		List<IECSNode> nodes = getECSNodeCollection();
		for(int i = 0; i < nodes.size();i++) {
			String name = nodes.get(i).getNodeName();
			String host = nodes.get(i).getNodeHost();
			Integer port = new Integer(nodes.get(i).getNodePort());
			ServiceLocation serverLocation = new ServiceLocation(name, host, port);
			serverLocations.add(serverLocation);
		}
		MD.setServerLocations(serverLocations);
		ServiceLocation ecsLocation = null;
		MD.setEcsLocation(ecsLocation);
		return MD;
	}
	
	public void printMD() {
		InfraMetadata MD = getMD();
		List<ServiceLocation> serverLocations = MD.getServerLocations();
		for(int i = 0; i < serverLocations.size();i++) {
			echo(serverLocations.get(i).serviceName + ":" +serverLocations.get(i).host + ":" + serverLocations.get(i).port);
		}
	}
	
	public void printHash() {
		List<IECSNode> nodes = getECSNodeCollection();
		for(int i = 0; i < nodes.size();i++) {
			IECSNode node = nodes.get(i);
			String from = node.getNodeHashRange()[0];
			String to = node.getNodeHashRange()[1];
			echo(node.getNodeName() + ":" + node.getNodeHost()+":"
					+node.getNodePort()+"["+ from + "~" + to + "]" );
		}
	}
	
	private void setConfigured() {
		setData("/configureStatus", "true");
	}
	public void setLaunchedNodes(List<IECSNode> launchedNodes) {
		String nodeRoot = "/nodes";
		byte[] emptyByte = null;
		String alias = "server_";
		create(nodeRoot, emptyByte, "-p");
		for(int i = 0; i < launchedNodes.size();i++) {
			IECSNode node = launchedNodes.get(i);
			String nodeDir = nodeRoot + "/" + alias + Integer.toString(i);
			create(nodeDir ,emptyByte,  "-p");
			create(nodeDir + "/NodeHost",node.getNodeHost().getBytes(StandardCharsets.UTF_8),  "-p");
			create(nodeDir + "/NodePort",Integer.toString(node.getNodePort()).getBytes(StandardCharsets.UTF_8),  "-p");
			create(nodeDir + "/from",node.getNodeHashRange()[0].getBytes(StandardCharsets.UTF_8),  "-p");
			create(nodeDir + "/to",node.getNodeHashRange()[1].getBytes(StandardCharsets.UTF_8),  "-p");
		}
		setConfigured();
	}
	
	/*
	 * locking primitives 
	 * Lock blocks
	 * trylock does not block and return true if succeed.
	 */
	
	private String lock_create() {
		byte[] emptyByte = null;
		String path = null;
		try {
			path = zk.create("/lock/spinlock/",emptyByte, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
		return path;
	}
	private void spinLock() {
		String n = lock_create();
		List<String> raceList = returnDirList("/lock/spinlock/");
		int array[] = new int[raceList.size()];
		for(int i=0;i<raceList.size();i++) {
			int entry = Integer.parseInt(raceList.get(i));
			array[i] = entry;
		}
		Arrays.sort(array);
		int winner = array[0];
		if(Integer.parseInt(Character.toString(n.charAt(n.length()-1)))==winner) {
			
		}
	}
	
	private boolean spinTryLock() {
		return false;
	}
	
	private void spinUnlock() {
		
	}

	public void reset() {
		List<String> rootDir = returnDirList("/");
		for(int i = 0; i < rootDir.size(); i++) {	
			String curr = rootDir.get(i);
			if(curr.equals("zookeeper")) {
				continue;
			}
			// rm -r everything
			deleteHeadRecursive("/" + curr);
		}
	}
}
