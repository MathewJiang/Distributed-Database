package testing;
<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
import junit.framework.TestCase;

import org.junit.Test;

import shared.metadata.InfraMetadata;

import app_kvServer.KVServer;
import app_kvServer.storage.Disk;
import app_kvServer.storage.ReplicaStore;

public class ReplicationTest extends TestCase {

	@Test
	public void testReplicateMessage() throws Exception {
		KVServer server = new KVServer();
		server.initTestOnly();
		server.replicateMessage("a", "b");
		assertEquals(KVServer.testReplicationCount, 2);
		server.resetMigrateResourcesTestOnly();
	}
	
	@Test
	public void testReplicateAllMessages() throws Exception {
		KVServer server = new KVServer();
		server.initTestOnly();
		String[] data = {"a", "b", "c", "d"};
		for (String s : data) {
			server.putKV(s, s);
		}
		server.replicate();
		assertEquals(KVServer.testReplicationCount, 2 * data.length);
		server.resetMigrateResourcesTestOnly();
	}
	
	@Test
	public void testReplicaMigrationLocal() throws Exception {
		KVServer server = new KVServer();
		server.initTestOnly();
	
		// In a test environment, all the keys belong to the test
		// server. Therefore during a replica local migration, everything
		// in the replica folder will be moved to Disk folder and 
		// replicated twice to mock "successors".
		InfraMetadata mock = new InfraMetadata();
		mock.addServiceLocation(KVServer.TEST_SERVER_MD);
		server.setClusterMD(mock);
		
		String[] data = {"a", "b", "c", "d"};
		for (String s : data) {
			ReplicaStore.putKV(s, s);
		}
		server.replicaMigrationLocal();
		for (String s : data) {
			assertEquals(Disk.getKV(s), s);
		}
		assertEquals(KVServer.testReplicationCount, data.length * 2);
		server.resetMigrateResourcesTestOnly();
	}
=======
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes


import java.io.IOException;
import java.util.ArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import client.KVStore;

import shared.messages.KVMessage.StatusType;
import shared.metadata.InfraMetadata;
import shared.metadata.ServiceLocation;
import app_kvECS.ECSClient;
import app_kvServer.KVServer;

import junit.framework.TestCase;

public class ReplicationTest extends TestCase {
	ECSClient ecsClient;
	KVStore client;
	KVServer server0;
	KVServer server1;
	KVServer server2;
	InfraMetadata md;
	
	@Before
	protected void setUp() throws Exception {
		ecsClient = new ECSClient();
		ecsClient.initECS();
		ecsClient.getECS().reset();
		ecsClient.setupNodes(1, "None", 0);
		
		md = new InfraMetadata();
		
		ServiceLocation ecsSl = new ServiceLocation("ecs", "127.0.0.1", 39678);
		ArrayList<ServiceLocation> sls = new ArrayList<ServiceLocation>();
		sls.add(new ServiceLocation("server_0", "127.0.0.1", 5000));
		sls.add(new ServiceLocation("server_1", "127.0.0.1", 5001));
		sls.add(new ServiceLocation("server_2", "127.0.0.1", 5002));
		
		md.setServerLocations(sls);
		md.setEcsLocation(ecsSl);
	}
	

	@Test(timeout=1000)
	public void testClient() {
		try {
			client = new KVStore();
			client.resetClusterHash(md);
			
			server0 = new KVServer(5000, 0, "None");
			server0.setClusterMD(md);
			server0.setServerInfo(new ServiceLocation("server_0", "127.0.0.1", 5000));
			server0.start();
			
			server1 = new KVServer(5001, 0, "None");
			server1.setClusterMD(md);
			server1.setServerInfo(new ServiceLocation("server_1", "127.0.0.1", 5001));
			server1.start();
			
			server2 = new KVServer(5002, 0, "None");
			server2.setClusterMD(md);
			server2.setServerInfo(new ServiceLocation("server_2", "127.0.0.1", 5000));
			server2.start();
			
			Thread.sleep(10);
			server0.setSuspended(false);
			server1.setSuspended(false);
			server2.setSuspended(false);
			
			
			client.setConnectTarget("127.0.0.1", 5001);
			client.connect();
			
			assertTrue(client.put("key1", "value1").getStatus().equals(StatusType.PUT_SUCCESS));
			assertTrue(client.get("key1").getStatus().equals(StatusType.GET_SUCCESS));
			
			client.disconnect();
		} catch (IOException e){
			fail("[testM1]IOException");
		} catch (InterruptedException e) {
			fail("[testM1]InterrupedException");
		} catch (Exception e) {
			fail("[testM1]Exception");
		}
	}
	
	
	
	@After
	public void cleanup() {
		ecsClient.getECS().broadast("SHUTDOWN");
		ecsClient.getECS().reset();
		ecsClient.getECS().deleteHeadRecursive("/nodes");
		ecsClient.getECS().deleteHeadRecursive("/configureStatus");
		ecsClient.shutdown();
		ecsClient.initECS();
	}
	

<<<<<<< Updated upstream
<<<<<<< Updated upstream
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
}
