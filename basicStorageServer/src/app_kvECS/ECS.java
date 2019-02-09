/***************************************************************
 * ECS:
 * 
 * ECS backend implementation
 ***************************************************************/

package app_kvECS;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

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

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
