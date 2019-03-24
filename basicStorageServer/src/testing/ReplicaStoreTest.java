package testing;

import java.io.IOException;

import org.junit.Test;

import app_kvServer.storage.ReplicaStore;
import junit.framework.TestCase;

public class ReplicaStoreTest extends TestCase {

	@Test
	public void testReplicaStore() {
		// Replication only directory handler.
		ReplicaStore.setDbName("/kvdb/" + "replicaStoreTest"
				+ "-replica");
		ReplicaStore.init();
		
		try {
			ReplicaStore.putKV("key1", "value1");
			String result = ReplicaStore.getKV("key1");
			
			assertTrue(result.equals("value1"));
			ReplicaStore.removeAllFiles();
		} catch (IOException e) {
			e.printStackTrace();
			fail("[ReplicaStoreTest]Exception has been thrown");
		}
	}

	@Test
	public void testRemoveReplicaStore() {
		// Replication only directory handler.
		ReplicaStore.setDbName("/kvdb/" + "replicaStoreTest"
				+ "-replica");
		ReplicaStore.init();
		
		try {
			ReplicaStore.putKV("key1", "value1");
			ReplicaStore.removeAllFiles();
			
			String result = ReplicaStore.getKV("key1");
			assertTrue(result == null);
		} catch (IOException e) {
			e.printStackTrace();
			fail("[ReplicaStoreTest]Exception has been thrown");
		}
	}
}
