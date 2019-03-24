package testing;
import junit.framework.TestCase;

import org.junit.Test;

import shared.metadata.InfraMetadata;
import shared.metadata.ServiceLocation;

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
	
	@Test
	public void testReplicaMigration() {
		KVServer server = new KVServer();
		try {
			server.initTestOnly();
			InfraMetadata og = new InfraMetadata();
			og.addServiceLocation(new ServiceLocation("test-only", "test-host", 0));
			og.addServiceLocation(new ServiceLocation("dummy1", "dummy1", 0));
			og.addServiceLocation(new ServiceLocation("dummy2", "dummy2", 0));
			og.addServiceLocation(new ServiceLocation("dummy3", "dummy3", 0));
			og.addServiceLocation(new ServiceLocation("dummy4", "dummy4", 0));
			og.addServiceLocation(new ServiceLocation("dummy5", "dummy5", 0));
			server.setClusterMD(og);
			
			int keyCount = 50;
			for (int i = 0; i < keyCount; i++) {
				server.putKV(Integer.toString(i), Integer.toString(i));
			}
			for (int i = 0; i < keyCount; i++) {
				server.putReplicaKV(Integer.toString(i), Integer.toString(i));
			}
			
			
			server.replicaMigration();
			
			// Should migrate 21 keys
			assertEquals(21, KVServer.testMigrateCount);
			server.resetMigrateResourcesTestOnly();
		} catch (Exception e) {
			e.printStackTrace();
			fail("[testReplicaMigration]Exception being thrown");
		}
	}
	
	@Test
	public void testRemoveReplicaKeys() {
		KVServer server = new KVServer();
		try {
			server.initTestOnly();
			InfraMetadata og = new InfraMetadata();
			og.addServiceLocation(new ServiceLocation("test-only", "test-host", 0));
			og.addServiceLocation(new ServiceLocation("dummy1", "dummy1", 0));
			og.addServiceLocation(new ServiceLocation("dummy2", "dummy2", 0));
			og.addServiceLocation(new ServiceLocation("dummy3", "dummy3", 0));
			og.addServiceLocation(new ServiceLocation("dummy4", "dummy4", 0));
			og.addServiceLocation(new ServiceLocation("dummy5", "dummy5", 0));
			server.setClusterMD(og);
			
			int keyCount = 50;
			for (int i = 0; i < keyCount; i++) {
				server.putKV(Integer.toString(i), Integer.toString(i));
			}
			for (int i = 0; i < keyCount; i++) {
				server.putReplicaKV(Integer.toString(i), Integer.toString(i));
			}
			
			
			server.removeReplicaKeys();
			
			// Should migrate 21 keys
			assertEquals(21, KVServer.testMigrateCount);
			server.resetMigrateResourcesTestOnly();
		} catch (Exception e) {
			e.printStackTrace();
			fail("[testReplicaMigration]Exception being thrown");
		}
	}
}
