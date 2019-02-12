/***************************************************************
 * ECS:
 * 
 * ECS backend implementation
 ***************************************************************/

package app_kvECS;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.Map;
import java.util.HashMap;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
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
		zk = new ZooKeeper(host, port, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				if (event.getState() == KeeperState.SyncConnected) {
                    connectionLatch.countDown();
                }
			}
			
		});
		
		connectionLatch.await();
		return zk;
	}
	
	/*****************************************************************************
	 * close:
	 * close the zk
	 *****************************************************************************/
	public void close() throws InterruptedException{
		zk.close();
	}
	
	public void create(String path, byte[] data, String mode) throws 
		KeeperException,InterruptedException {
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
		zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,CMode);
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
	public void deleteHead(String target) {
		try {
			zk.delete(target,zk.exists(target,true).getVersion());
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void setData(String path, byte[] data) {
		try {
			zk.setData(path, data, -1);
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
			return (zk.getData(path, false, null)).toString();
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
}