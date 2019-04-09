/***************************************************************
 * ECS:
 * 
 * ECS backend implementation
 ***************************************************************/

package app_kvECS;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import shared.ConsistentHash;
import shared.messages.KVAdminMessage;
import shared.messages.KVAdminMessage.KVAdminMessageType;
import shared.metadata.InfraMetadata;
import shared.metadata.ServiceLocation;
import ecs.ECSNode;
import ecs.IECSNode;

public class ECS {

	private String ssh_content = "";
	private String ssh_location = "./ssh.cmd";
	private ZooKeeper zk;
	CountDownLatch connectionLatch = new CountDownLatch(1);
	spinlock globalLock = new spinlock("globalLock");
	public spinlock ackLock = new spinlock("ackLock");
	String prevCmd = "null";
	private static Logger logger = Logger.getRootLogger();
	String ECSip = "";

	// String logConfigFileLocation = "./";

	/*****************************************************************************
	 * connect: connect the zk
	 * 
	 * @param host
	 *            name of the host
	 * @param port
	 *            port number of the host
	 * @return connected zk
	 *****************************************************************************/
	public ZooKeeper connect(String host, int port) throws IOException,
			InterruptedException {
		String connectString = host + ":" + port;
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
			if (zk.exists("/lock", true) == null) {
				create("/lock", emptyByte, "-p");
			}
			if (zk.exists("/lock/spinlock", true) == null) {
				create("/lock/spinlock", emptyByte, "-p");
			}

		} catch (KeeperException e) {
			echo("concurrent trying to make spinlock");
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		connectionLatch.await();
		echo("Connected to " + host + ":" + port);
		ECSip = host;
		return zk;
	}
	
	public String screen(int start, int end, boolean attack) {
		String host = "127.0.0.1";
		int port = end;
		String connectString = "";
		final boolean[] valid = new boolean[1];
		valid[0] = false;
		while(port >= start) {
			connectString = host + ":" + port;
			System.out.println("Trying " + connectString);
			
			try {
				Watcher watch = new Watcher() {
					@Override
					public void process(WatchedEvent event) {
						if (event.getState() == KeeperState.SyncConnected) {
							connectionLatch.countDown();
							valid[0] = true;
						} else {
							System.out.println(event);
						}
					}
				};
				zk = new ZooKeeper(connectString, 500, watch);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			try {
				connectionLatch.await(25, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				zk.close();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if(valid[0]) {
				break;
			} else {
				port--;
			}
		}
		if(valid[0]) {
			echo("Connected to " + host + ":" + port);
			ECSip = host;
		} else {
			System.out.println("not found");
			connectString = "";
		}
		return connectString;
	}

	/*****************************************************************************
	 * close: close the zk
	 *****************************************************************************/
	public void close() throws InterruptedException {
		zk.close();
	}

	public String create(String path, byte[] data, String mode) {
		CreateMode CMode = CreateMode.EPHEMERAL;
		switch (mode) {
		case "-p":
			CMode = CreateMode.PERSISTENT;
			break;
		case "-ps":
			CMode = CreateMode.EPHEMERAL_SEQUENTIAL;
			break;
		case "-e":
			CMode = CreateMode.EPHEMERAL;
			break;
		case "-es":
			CMode = CreateMode.EPHEMERAL_SEQUENTIAL;
			break;
		}
		String result = null;
		try {
			result = zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CMode);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}

	public void echo(String line) {
		// System.out.println(line);
		logger.info(line);
	}

	public void printPath(String path) {
		try {
			List<String> Children = zk.getChildren(path, false);
			Iterator<String> it = Children.iterator();
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
			Iterator<String> it = Children.iterator();
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
			echo("delete " + target);
			zk.delete(target, zk.exists(target, true).getVersion());
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
			String data = new String((zk.getData(path, false, null)),
					StandardCharsets.US_ASCII);
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
		Iterator<String> it = curr_children.iterator();
		while (it.hasNext()) {
			String curr = it.next();
			if (target.equals("/")) {
				continue;
			}
			if (target.endsWith("/")) {
				target = target.substring(0, target.length() - 1);
			}
			if (returnDirList(target).size() != 0) {
				if (!target.endsWith("/")) {
					target += "/";
				}
				deleteHeadRecursive(target + curr);
			}
		}
		if (target.endsWith("/")) {
			target = target.substring(0, target.length() - 1);
		}
		if (returnDirList(target).size() == 0) {

			try {
				// echo("real delete " + target);
				zk.delete(target, zk.exists(target, true).getVersion());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (KeeperException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	public void deleteHeadRecursiveKeepHead(String target) {
		echo("delete " + target);
		List<String> curr_children = returnDirList(target);
		Iterator<String> it = curr_children.iterator();
		while (it.hasNext()) {
			String curr = it.next();
			if (target.equals("/")) {
				continue;
			}
			if (target.endsWith("/")) {
				target = target.substring(0, target.length() - 1);
			}
			if (returnDirList(target).size() != 0) {
				if (!target.endsWith("/")) {
					target += "/";
				}
				deleteHeadRecursive(target + curr);
			}
		}
	}
	public boolean configured() {
		try {
			if (zk.exists("/configureStatus", false) == null) {
				zk.create("/configureStatus",
						("false").getBytes(StandardCharsets.UTF_8),
						ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				return false;
			} else {
				return (getData("/configureStatus").equals("true"));
			}

		} catch (KeeperException e) {
			echo("concurrent trying to make configureStatus");
			return true;
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return false;
	}

	public List<IECSNode> getECSNodeCollection() {
		// echo("Info: get from ECS repo");
		List<String> servers = returnDirList("/nodes");
		List<IECSNode> nodelist = new ArrayList<IECSNode>();
		for (int i = 0; i < servers.size(); i++) {
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
		for (int i = 0; i < nodes.size(); i++) {
			String name = nodes.get(i).getNodeName();
			String host = nodes.get(i).getNodeHost();
			Integer port = new Integer(nodes.get(i).getNodePort());
			ServiceLocation serverLocation = new ServiceLocation(name, host,
					port);
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
		if (serverLocations == null) {
			echo("[ECS.java/printMD]MD empty!");
			return;
		}

		for (int i = 0; i < serverLocations.size(); i++) {
			echo(serverLocations.get(i).serviceName + ":"
					+ serverLocations.get(i).host + ":"
					+ serverLocations.get(i).port);
		}
	}

	public void printHash() {
		List<IECSNode> nodes = getECSNodeCollection();
		for (int i = 0; i < nodes.size(); i++) {
			IECSNode node = nodes.get(i);
			String from = node.getNodeHashRange()[0];
			String to = node.getNodeHashRange()[1];
			echo(node.getNodeName() + ":" + node.getNodeHost() + ":"
					+ node.getNodePort() + "[" + from + "~" + to + "]");
		}
	}

	public void setConfigured(boolean condition) {
		if (condition) {
			setData("/configureStatus", "true");
		} else {
			setData("/configureStatus", "false");
		}
	}

	public List<IECSNode> setLaunchedNodes(List<IECSNode> launchedNodes) {
		String nodeRoot = "/nodes";
		byte[] emptyByte = null;
		String alias = "server_";
		try {
			if (zk.exists("/nodes", null) == null) {
				create(nodeRoot, emptyByte, "-p");
			}
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
		@SuppressWarnings("unused")
		byte[] nullByte = "null".getBytes(StandardCharsets.UTF_8);
		List<IECSNode> aliasedNodes = new ArrayList<IECSNode>(launchedNodes);
		for (int i = 0; i < launchedNodes.size(); i++) {
			IECSNode node = launchedNodes.get(i);
			String nodeDir = nodeRoot + "/" + alias + Integer.toString(i);
			create(nodeDir, emptyByte, "-p");
			create(nodeDir + "/NodeHost",
					node.getNodeHost().getBytes(StandardCharsets.UTF_8), "-p");
			create(nodeDir + "/NodePort", Integer.toString(node.getNodePort())
					.getBytes(StandardCharsets.UTF_8), "-p");
			create(nodeDir + "/from",
					node.getNodeHashRange()[0].getBytes(StandardCharsets.UTF_8),
					"-p");
			create(nodeDir + "/to",
					node.getNodeHashRange()[1].getBytes(StandardCharsets.UTF_8),
					"-p");
			create(nodeDir + "/cmd", "null".getBytes(StandardCharsets.UTF_8),
					"-p");
			create(nodeDir + "/state", "init".getBytes(StandardCharsets.UTF_8),
					"-p");
			((ECSNode) aliasedNodes.get(i)).setNodeName(alias
					+ Integer.toString(i));
		}
		setConfigured(true);
		return aliasedNodes;
	}

	public void addOneLaunchedNodes(IECSNode node) {
		String nodeRoot = "/nodes";
		byte[] emptyByte = null;
		String alias = "server_";
		try {
			if (zk.exists("/nodes", null) == null) {
				create(nodeRoot, emptyByte, "-p");
			}
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
		@SuppressWarnings("unused")
		byte[] nullByte = "null".getBytes(StandardCharsets.UTF_8);
		String nodeDir = nodeRoot + "/" + node.getNodeName();
		create(nodeDir, emptyByte, "-p");
		create(nodeDir + "/NodeHost",
				node.getNodeHost().getBytes(StandardCharsets.UTF_8), "-p");
		create(nodeDir + "/NodePort", Integer.toString(node.getNodePort())
				.getBytes(StandardCharsets.UTF_8), "-p");
		create(nodeDir + "/from",
				node.getNodeHashRange()[0].getBytes(StandardCharsets.UTF_8),
				"-p");
		create(nodeDir + "/to",
				node.getNodeHashRange()[1].getBytes(StandardCharsets.UTF_8),
				"-p");
		create(nodeDir + "/cmd", "null".getBytes(StandardCharsets.UTF_8), "-p");
		create(nodeDir + "/state", "init".getBytes(StandardCharsets.UTF_8),
				"-p");

		setConfigured(true);
		return;
	}

	/*
	 * locking primitives Lock blocks trylock does not block and return true if
	 * succeed.
	 */

	private String lock_create(String lockDir) {
		byte[] emptyByte = null;
		String path = null;
		try {
			path = zk.create(lockDir + "/", emptyByte,
					ZooDefs.Ids.OPEN_ACL_UNSAFE,
					CreateMode.EPHEMERAL_SEQUENTIAL);
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
		return path;
	}

	private CountDownLatch Latch = null;

	@SuppressWarnings("unused")
	private String getTypeName(Watcher.Event.EventType type) {
		if (type == EventType.None) {
			return "None";
		} else if (type == EventType.NodeChildrenChanged) {
			return "NodeChildrenChanged";
		} else if (type == EventType.NodeCreated) {
			return "NodeCreated";
		} else if (type == EventType.NodeDataChanged) {
			return "NodeDataChanged";
		} else if (type == EventType.NodeDeleted) {
			return "NodeDeleted";
		}

		return "not valid";
	}

	private boolean watchStringDelete(final String path) {
		// echo("watching path " + path);
		final boolean valid[] = { true };
		Latch = new CountDownLatch(1);
		Stat check = null;
		try {
			check = zk.exists(path, new Watcher() {
				public void process(WatchedEvent e) {
					if (e.getType() == EventType.NodeDeleted) {
						Latch.countDown();
					} else {
						valid[0] = false;
						Latch.countDown();
					}
				}
			});
		} catch (KeeperException e1) {
			e1.printStackTrace();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		if (check != null) {
			try {
				Latch.await();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			Latch = null;
			return valid[0];
		}
		Latch.countDown();
		return true;
	}

	private int watchNodeChildren(final String path, int th, int timeoutSeconds) {
		echo("watching " + path + "count is " + th);
		final int count[] = { returnDirList(path).size() };
		final int valid[] = { 0 };
		if (count[0] == th) {
			return th; // no more event is going to happen
		}
		Latch = new CountDownLatch(1);
		try {
			count[0] = zk.getChildren(path, new Watcher() {
				public void process(WatchedEvent e) {
					echo(getTypeName(e.getType()));
					if (e.getType() == EventType.NodeChildrenChanged) {
						count[0] = returnDirList(path).size();
						valid[0] = 1;
						Latch.countDown();
					} else {
						valid[0] = -1;
						Latch.countDown();
					}
				}
			}).size();
		} catch (KeeperException e1) {
			e1.printStackTrace();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		try {
			Latch.await(timeoutSeconds, TimeUnit.SECONDS);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		echo("action " + path + " count is " + count[0]);
		if (valid[0] == 1) {
			return count[0]; // valid
		} else if (valid[0] == 0) {
			return 0; // timeout
		} else {
			return -1; // invalid
		}
	}

	private String watchStringDataChanged(final String path) {
		final boolean valid[] = { true };
		Latch = new CountDownLatch(1);
		byte[] data = null;
		try {
			data = zk.getData(path, new Watcher() {
				public void process(WatchedEvent e) {
					if (e.getType() == EventType.NodeDataChanged) {
						Latch.countDown();
					} else {
						valid[0] = false;
						Latch.countDown();
					}
				}
			}, null);
		} catch (KeeperException e1) {
			e1.printStackTrace();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		try {
			Latch.await();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		Latch = null;
		if (valid[0]) {
			if (data == null) {
				return "data == null";
			}
			String result = new String(data, StandardCharsets.US_ASCII);
			return result;
		} else {
			return "";
		}
	}

	public void lock() {
		globalLock.lock();
	}

	public void unlock() {
		globalLock.unlock();
	}

	public class spinlock {
		private String lockPath = "";
		private String lockDir = "";
		private int cutIndex = 0;

		public spinlock(String lockname) {
			lockDir = "/lock/" + lockname;
			cutIndex = lockDir.length() + 1;
		}

		public void lock() {
			String n = lock_create(lockDir); // full path
			// echo("n = " + n);
			while (true) {
				List<String> raceList = returnDirList(lockDir);
				int array[] = new int[raceList.size()];
				for (int i = 0; i < raceList.size(); i++) {
					// echo(raceList.get(i));
					int entry = Integer.parseInt(raceList.get(i));
					// echo(i+": "+entry);
					array[i] = entry;
				}
				Arrays.sort(array);

				int winner = array[0];
				int ticket = Integer.parseInt(n.substring(cutIndex));
				if (ticket == winner || ticket == 0) {
					setData(n, "true");
					// locked
					lockPath = n;
					return;
				} else {
					// wait for previous competitor ticket - 1
					int waitFor = ticket - 1;
					String appendZero = ("0000000000" + waitFor);
					String fillMSBZero = appendZero.substring(appendZero
							.length() - 10);
					echo("waitfor " + fillMSBZero + " I am " + n);
					watchStringDelete(lockDir + "/" + fillMSBZero);
				}
			}
		}

		public boolean trylock() {
			String n = lock_create(lockDir); // full path
			List<String> raceList = returnDirList("lockPath");
			int array[] = new int[raceList.size()];
			for (int i = 0; i < raceList.size(); i++) {
				int entry = Integer.parseInt(raceList.get(i));
				array[i] = entry;
			}
			Arrays.sort(array);
			int winner = array[0];
			int ticket = Integer.parseInt(Character.toString(n.charAt(n
					.length() - 1)));
			if (ticket == winner) {
				setData(n, "true");
				lockPath = n;
				// locked
				return true;
			}
			return false;
		}

		public void unlock() {
			try {
				zk.delete(lockPath, zk.exists(lockPath, true).getVersion());
			} catch (InterruptedException | KeeperException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	public void init() {
		try {
			if (zk.exists("/lock", true) == null) {
				zk.create("/lock", ("false").getBytes(StandardCharsets.UTF_8),
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			if (zk.exists("/lock/globalLock", true) == null) {
				zk.create("/lock/globalLock",
					("false").getBytes(StandardCharsets.UTF_8),
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			if(zk.exists("/lock/ackLock", true) == null) {
				zk.create("/lock/ackLock",
					("false").getBytes(StandardCharsets.UTF_8),
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			if (zk.exists("/nodes", true) == null) {
				zk.create("/nodes", ("false").getBytes(StandardCharsets.UTF_8),
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			if(zk.exists("/ack", true) == null) {
				zk.create("/ack", ("false").getBytes(StandardCharsets.UTF_8),
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			if (zk.exists("/register", true) == null) {
				zk.create("/register", ("false").getBytes(StandardCharsets.UTF_8),
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return;
	}

	public void reset() {
		List<String> rootDir = returnDirList("/");
		for (int i = 0; i < rootDir.size(); i++) {
			String curr = rootDir.get(i);
			if (curr.equals("zookeeper")) {
				continue;
			}
			
			// rm -r everything
			deleteHeadRecursive("/" + curr);
		}
	}

	private String getCmdFromZk(String serverName) {
		String result = null;
		String check = getData("/nodes/" + serverName + "/cmd");
		if (!check.equals(prevCmd)) {
			// try to recover missed cmd
			prevCmd = check;
			return check;
		}
		result = watchStringDataChanged("/nodes/" + serverName + "/cmd");
		result = getData("/nodes/" + serverName + "/cmd");
		prevCmd = result;
		return result;
	}

	private KVAdminMessageType StringToKVAdminMessageType(String a) {
		switch(a) {// ecs.setCmd(newNode.getNodeName(), "SYNC");

			case "START":
				return KVAdminMessageType.START;
			case "STOP":
				return KVAdminMessageType.STOP;
			case "SHUTDOWN":
				return KVAdminMessageType.SHUTDOWN;
			case "UPDATE":
				return KVAdminMessageType.UPDATE;
			case "LOCK_WRITE":
				return KVAdminMessageType.LOCK_WRITE;
			case "UNLOCK_WRITE":
				return KVAdminMessageType.UNLOCK_WRITE;
			case "SYNC":
				return KVAdminMessageType.SYNC;
			case "REPORT":
				return KVAdminMessageType.REPORT;
			case "LOCK_WRITE_REMOVE_RECEVIER":
				return KVAdminMessageType.LOCK_WRITE_REMOVE_RECEVIER;
			case "LOCK_WRITE_REMOVE_SENDER":
				return KVAdminMessageType.LOCK_WRITE_REMOVE_SENDER;
			case "RESTORE":
				return KVAdminMessageType.RESTORE;
			case "REPLICA_MIGRATE":
				return KVAdminMessageType.REPLICA_MIGRATE;
			case "REREPLICATION":
				return KVAdminMessageType.REREPLICATION;
			case "KILL":
				return KVAdminMessageType.KILL;
			case "REPLICA_LOCAL_MIGRATE":
				return KVAdminMessageType.REPLICA_LOCAL_MIGRATE;
		}
		return null;
	}

	public String KVAdminMessageTypeToString(KVAdminMessageType a) {
		if (a == KVAdminMessageType.START) {
			return "START";
		} else if (a == KVAdminMessageType.STOP) {
			return "STOP";
		} else if (a == KVAdminMessageType.SHUTDOWN) {
			return "SHUTDOWN";
		} else if (a == KVAdminMessageType.UPDATE) {
			return "UPDATE";
		} else if (a == KVAdminMessageType.LOCK_WRITE) {
			return "LOCK_WRITE";
		} else if (a == KVAdminMessageType.UNLOCK_WRITE) {
			return "UNLOCK_WRITE";
		} else if (a == KVAdminMessageType.SYNC) {
			return "SYNC";
		} else if (a == KVAdminMessageType.REPORT) {
			return "REPORT";
		} else if (a == KVAdminMessageType.LOCK_WRITE_REMOVE_RECEVIER) {
			return "LOCK_WRITE_REMOVE_RECEVIER";
		} else if (a == KVAdminMessageType.LOCK_WRITE_REMOVE_SENDER) {
			return "LOCK_WRITE_REMOVE_SENDER";
		} else if(a == KVAdminMessageType.RESTORE) {
			return "RESTORE";
		} else if(a == KVAdminMessageType.REPLICA_MIGRATE) {
			return "REPLICA_MIGRATE";
		} else if(a == KVAdminMessageType.REREPLICATION) {
			return "REREPLICATION";
		} else if(a == KVAdminMessageType.KILL) {
			return "KILL";
		} else if(a == KVAdminMessageType.REPLICA_LOCAL_MIGRATE) {
			return "REPLICA_LOCAL_MIGRATE";
		}
		return "INVALID KVAdminMessageType";
	}

	public KVAdminMessage getCmd(String serverName) {
		// ackLock.lock();
		KVAdminMessage adminMsg = new KVAdminMessage();
		adminMsg.setKVAdminMessageType(StringToKVAdminMessageType(getCmdFromZk(serverName)));
		adminMsg.MD = getMD();
		// setCmd(serverName, "null");
		// ackLock.unlock();
		return adminMsg;
	}

	public void setCmd(String serverName, String command) {
		// ackLock.lock();
		echo(serverName + ": " + command);
		setData("/nodes/" + serverName + "/cmd", command);
		// ackLock.unlock();
	}

	private boolean exists(String entry) {
		try {
			if (zk.exists(entry, true) != null) {
				return true;
			}
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return false;
	}

	public void broadast(String cmd) {
		if (!exists("/nodes")) {
			return;
		}
		InfraMetadata latestMD = getMD();
		List<ServiceLocation> allServers = latestMD.getServerLocations();
		for (int i = 0; i < allServers.size(); i++) {
			String serverName = allServers.get(i).serviceName;
			setCmd(serverName, cmd);
		}
	}

	public void makeSureAckIsSet() {
		try {
			if (zk.exists("/ack", true) == null) {
				create("/ack", null, "-p");
				return;
			}
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void makeSureAckIssueIsSet(String issue) {
		try {
			if (zk.exists("/ack/" + issue, true) == null) {
				create("/ack/" + issue, null, "-p");
				while(zk.exists("/ack/" + issue, true) == null) {}
				return;
			}
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void ack(String serverName, String action) {
		echo(serverName + " acking " + action);
		if (exists("/ack")) {
			if (exists("/ack/" + action)) {
				create("/ack/" + action + "/" + serverName, null, "-p");
			}
		}
	}

	public boolean waitAck(String action, int countDown, int timeOutSeconds) {
		echo("waitAck " + action + " " + countDown);
		int result = 0;
		Instant start = Instant.now();
		boolean isValid = true;
		
		while (result != countDown) {
			result = watchNodeChildren("/ack/" + action, countDown, 1);
			if (result == 0) {
				Instant end = Instant.now();
				Duration timeElapsed = Duration.between(start, end);
				if (timeElapsed.toMillis() / 1000 > timeOutSeconds) {
					echo("Warn: timeout (" + timeOutSeconds + ")");
					isValid = false;
					break;
				}
			}
		}
		;
		echo("clear " + action);
		// clear the wait
		deleteHeadRecursive("/ack/" + action);
		return isValid;
	}

	public void waitAckSetup(String action) {
		makeSureAckIsSet();
		makeSureAckIssueIsSet(action);
	}

	public boolean inited() {
		try {
			if (zk.exists("/nodes", false) != null) {
				return true;
			}
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
		return false;
	}

	public void refreshHash(ConsistentHash hashRing) {
		String nodeRoot = "/nodes";
		String alias = "server_";

		for (ServiceLocation sl : hashRing.getHashRing().values()) {
			int lastIndex = sl.serviceName.length() - 1;
			String nodeDir = nodeRoot + "/" + alias
					+ sl.serviceName.charAt(lastIndex);
			String range[];

			try {
				range = hashRing.getHashRange(sl);
				echo(range[0] + " ~ " + range[1]);
				deleteHead(nodeDir + "/from");
				deleteHead(nodeDir + "/to");
				create(nodeDir + "/from",
						range[0].getBytes(StandardCharsets.UTF_8), "-p");
				create(nodeDir + "/to",
						range[1].getBytes(StandardCharsets.UTF_8), "-p");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public boolean existsLeader() {
		try {
			return zk.exists("/leader", true) != null;
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
		return false;
	}
	
	public void setLeader(String leader, String ip) {
		logger.info("Setting leader: " + leader + ", " + ip);
		try {
			if (zk.exists("/leader", true) == null) {
				create("/leader", null, "-p");
			}
			if (zk.exists("/leader/servername", true) == null) {
				create("/leader/servername", null, "-p");
			}
			if (zk.exists("/leader/ip", true) == null) {
				create("/leader/ip", null, "-p");
			}

		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		setData("/leader/servername", leader);
		setData("/leader/ip", ip);
	}

	public String getLeader() {
		String s = getData("/leader/servername");
		logger.info("Got leader: " + s);
		return s;
	}
	public void register(String myName) { // must register before ack server started
		try {
			if (zk.exists("/register", true) == null) {
			    // may race condition
				create("/register", null, "-p");
			}
			create("/register/" + myName, null, "-s");
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return;
	}
	private String diffNodes(List<String> liveNodeList, List<String> presistent_records) {
		presistent_records.removeAll(liveNodeList);
		if(presistent_records.size() != 1) {
			// echo("server down count is " + presistent_records.size() );
		} else {
			// this is legit
			return presistent_records.get(0);
		}
		if(presistent_records.size() == 0) {
			return "";
		} else {
			return "more than 1 server!";
		}
	}
	public void monitor_registry() {
		echo("monitor_registry started");
		List<String> liveNodeList = null;
		ECSClient ECSClientInterface = new ECSClient();
		
		while (true) {
			final CountDownLatch monitorLatch = new CountDownLatch(1);
			 try {
				liveNodeList = zk.getChildren("/register", new Watcher() {
				public void process(WatchedEvent e) {
					// System.out.println(e.getType());
					monitorLatch.countDown();
					}
				});
			} catch (KeeperException | InterruptedException e) {
				e.printStackTrace();
			}
			try {
				monitorLatch.await(5, TimeUnit.SECONDS);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			ackLock.lock();
			try {
				if (zk.exists("/nodes", true) == null) {
					echo("detects reset -all");
				}
			} catch (KeeperException | InterruptedException e) {
				e.printStackTrace();
			}
			try {
				liveNodeList = zk.getChildren("/register", null);
			} catch (KeeperException | InterruptedException e1) {
				e1.printStackTrace();
			} // just recheck
			String crushed_server = diffNodes(liveNodeList, returnDirList("/nodes"));
			if(crushed_server.equals("more than 1 server!")) {
				echo("failed to detect crushed_server");
				ackLock.unlock();
			} else if(crushed_server.equals("")) {
				ackLock.unlock();
			} else {
				
				// do something here
				echo("detected " + crushed_server + " crushed");
		    
		    	lock();
		    	ConsistentHash oldHash = new ConsistentHash();
		    	InfraMetadata oldMD = getMD();
		    	oldHash.addNodesFromInfraMD(oldMD);
		    	
		    	ServiceLocation crushed_server_rebuilt = null;
		    	for(int i = 0; i < oldMD.getServerLocations().size(); i++) {
		    		if(oldMD.getServerLocations().get(i).serviceName.equals(crushed_server)) {
		    			crushed_server_rebuilt = oldMD.getServerLocations().get(i);
		    			break;
		    		}
		    	}
		    	
		    	// changing MD
		    	deleteHeadRecursive("/nodes/" + crushed_server);
		    	
		    	ConsistentHash hashRing = new ConsistentHash();	
		    	InfraMetadata MD = getMD();
		    	hashRing.addNodesFromInfraMD(MD); // hash ring up to data
		    	refreshHash(hashRing); // ECS completely updated
		    	// migration
		    	// Make sure system is ready to shuffle in new metadata state.
		    	
		    	
		    	
				waitAckSetup("sync");
				
				broadast("SYNC");
				waitAck("sync", MD.getServerLocations().size(), 50);
				try {
				waitAckSetup("remove_shuffle");
				setCmd(oldHash.getSuccessor(crushed_server_rebuilt).serviceName, "REPLICA_LOCAL_MIGRATE");
				waitAck("remove_shuffle", 1, 50);
				waitAckSetup("remove_shuffle");
				setCmd(oldHash.getPredeccessor(crushed_server_rebuilt).serviceName, "REREPLICATION");
				waitAck("remove_shuffle", 1, 50);
				waitAckSetup("remove_shuffle");
				setCmd(oldHash.getPredeccessor(oldHash.getPredeccessor(crushed_server_rebuilt)).serviceName, "REREPLICATION");
				waitAck("remove_shuffle", 1, 50);
				} catch (Exception e) {}	
				
				refreshHash(hashRing);
				// ecs.waitAckSetup("migrate");
				// ecs.broadast("LOCK_WRITE");
							
				// unlock(); // keep locking as well needed
		    	
		    	// addNode from pool
		    	
		    	String recoverServerName = getNewServerName();
		    	System.out.println("recoverServerName = " + recoverServerName);
		    	
		    	ServiceLocation spot = get_a_slot();
		    	spot.serviceName = recoverServerName; // aliasing
		    	InfraMetadata new_MD = getMD();
				List<ServiceLocation> tmp = new_MD.getServerLocations();
				tmp.add(spot);
				new_MD.setServerLocations(tmp);
				hashRing.addNodesFromInfraMD(new_MD);
			
				try {
				String affectedServerName = hashRing.getSuccessor(spot).serviceName;
				setCmd(affectedServerName, "LOCK_WRITE");
				} catch (Exception e) {}
			
				try {
					IECSNode newNode = new ECSNode(spot.serviceName, spot.host, spot.port, hashRing.getHashRange(spot));
					addOneLaunchedNodes(newNode);
					refreshHash(hashRing);
					waitAckSetup("launched");
					
					String cacheStrategy = "None";
					int cacheSize = 0;
					
					clear_script();
					launch(spot.host, spot.serviceName, ECSip, cacheStrategy, cacheSize);
					run_script();
					waitAck("launched", 1, 50); // new node launched
					waitAckSetup("migrate");
					unlock(); // allow effected node to migrate
					waitAck("migrate", 1, 50); // internal unlock -> new nodes migrated
					waitAckSetup("sync");
					broadast("SYNC"); // Including launched new server
					waitAck("sync", new_MD.getServerLocations().size(), 50); 
					
					// Replica storage migration based on new metadata server received.
					broadast("REPLICA_MIGRATE");
					waitAckSetup("remove_shuffle");
					waitAck("remove_shuffle", new_MD.getServerLocations().size(), 50); // internal unlock -> new nodes migrated
			    	
					ackLock.unlock();
				} catch (Exception e){}
				
			}
		}
	}
	public String getNewServerName() {
    	List<String> serverList = returnDirList("/nodes");
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
    	
    	int serial = getMD().getServerLocations().size();
    	String name = "server_" + serial;
    	return name;
    }
	public void launch(String remoteIP, String serverName, String ECSip, String strategy, int cache_size) {
    	Runtime run = Runtime.getRuntime();
    	Process proc;
    	ECSClient ECSClientInterface = new ECSClient();
    	ECSClientInterface.set_workDir();
    	ECSClientInterface.info("launch(ip = " + remoteIP + " port = " + ECSip + ")");
    	if(remoteIP.equals("127.0.0.1") || remoteIP.equals("localhost")) {
    		try {
				proc = Runtime.getRuntime().exec(ECSClientInterface.nossh_launch_array(serverName, ECSip, strategy, cache_size));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	} else {
    		
    		/*try {
    			proc = Runtime.getRuntime().exec(ssh_launch_array(remoteIP, serverName, ECSport, strategy, cache_size));
    		} catch (IOException e) {
    			e.printStackTrace();
    		}*/
    		String cmd = ECSClientInterface.ssh_launch_array(remoteIP, serverName, ECSip, strategy, cache_size);
    		/*try {
				Thread.sleep(2);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}*/
    		ssh_content += cmd + "\nsleep 3 \n";
    	}
    }
	private void clear_script() {
		ssh_content = "";
	}
	private void run_script() {
		String cmdFile = ssh_location;
		File search = new File(cmdFile);
		if (search.exists()) {
		} else {
			try {
				search.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		PrintWriter key_file;
		try {
			key_file = new PrintWriter(cmdFile);
			key_file.print(ssh_content);
			key_file.flush();
			key_file.close();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
    	String[] cmdScript = new String[]{"/bin/bash", cmdFile}; 
		echo("Running " + cmdFile);
		Process procScript;
		try {
			procScript = Runtime.getRuntime().exec(cmdScript);
			try {
				procScript.waitFor();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	public ServiceLocation get_a_slot() {
		ServiceLocation slot = null;
		String ecs_config = "./resources/config/ecs.config";
	    InfraMetadata latestMD = getMD();
    	List<ServiceLocation> slotTaken = latestMD.getServerLocations();
    	
    	try {
			List<ServiceLocation> totalPool = InfraMetadata.fromConfigFile(ecs_config).getServerLocations();
			List<ServiceLocation> aliasedSlotTaken = new ArrayList<ServiceLocation>();
			for(int i = 0; i < slotTaken.size();i++) {
				System.out.println("host: " + slotTaken.get(i).host + " port: " + slotTaken.get(i).port);
				for(int j = 0; j < totalPool.size();j++) {
					if(slotTaken.get(i).host.equals(totalPool.get(j).host)){
						if((int)slotTaken.get(i).port == (int)totalPool.get(j).port) {
							// taken
							//System.out.println("Hit");
							aliasedSlotTaken.add(totalPool.get(j));
						} else {
							//System.out.println(slotTaken.get(i).port +" " + totalPool.get(j).port);
						}
					}
				}
			}
			System.out.println("totalPool[" + totalPool.size() + "]");
			System.out.println("unaliasedSlotTaken[" + aliasedSlotTaken.size() + "]");
			totalPool.removeAll(aliasedSlotTaken);
			System.out.println("totalPool[" + totalPool.size() + "]");
			if(slotTaken.get(0)!=null) {
				slot = totalPool.get(0);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return slot;
	}
	public void Squeeze() {
		while(true) {
			byte[] array = new byte[100];
		    new Random().nextBytes(array);
		    try {
				if (zk.exists("/hot", true) == null) {
					create("/hot", array, "-p");
				}
			} catch (KeeperException | InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			try {
				zk.setData("/hot", array, -1);
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	

}
