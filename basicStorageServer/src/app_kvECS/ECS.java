/***************************************************************
 * ECS:
 * 
 * ECS backend implementation
 ***************************************************************/

package app_kvECS;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.BasicConfigurator;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.LoggerFactory;
public class ECS {

	private ZooKeeper zk;
	CountDownLatch connectionLatch = new CountDownLatch(1);

	
	/*****************************************************************************
	 * connect:
	 * connect the zk
	 * @param	host	name of the host
	 * @param	port	port number of the host
	 * @return 			connected zk
	 *****************************************************************************/
	public ZooKeeper connect(String host, int port) throws IOException, InterruptedException {
		BasicConfigurator.configure();
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
	
	public void create(String path, byte[] data) throws 
		KeeperException,InterruptedException {
		zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
	}
	
	/*public void Executor(String hostPort, String znode, String filename, String exec[]) throws KeeperException, IOException {
        dm = new DataMonitor(zk, znode, null, this);
    }*/

    /*public void run() {
        try {
            synchronized (this) {
                while (!dm.dead) {
                    wait();
                }
            }
        } catch (InterruptedException e) {
        }
    }*/

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
