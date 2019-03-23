package testing;

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
}
