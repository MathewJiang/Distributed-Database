package testing;

import junit.framework.TestCase;

import org.junit.Test;

import app_kvServer.KVServer;
import app_kvServer.storage.Disk;
import shared.metadata.InfraMetadata;
import shared.metadata.ServiceLocation;

public class MigrationTest extends TestCase {

	@Test
	public void testMigrateAll() throws Exception {
		KVServer server = new KVServer();
		server.initTestOnly();
		int keyCount = 500;
		for (int i = 0; i < keyCount; i++) {
			server.putKV(Integer.toString(i), Integer.toString(i));
		}
		InfraMetadata mock = new InfraMetadata();
		mock.addServiceLocation(new ServiceLocation("dummy", "dummy", 0));
		server.migrateAll(mock);

		assertEquals(keyCount, KVServer.testMigrateCount);
		server.resetMigrateResourcesTestOnly();
	}
	
	@Test
	public void testMigrateWithNewMD() throws Exception {
		KVServer server = new KVServer();
		server.initTestOnly();
		InfraMetadata og = new InfraMetadata();
		og.addServiceLocation(new ServiceLocation("dummy", "dummy", 0));
		server.setClusterMD(og);
		
		int keyCount = 500;
		for (int i = 0; i < keyCount; i++) {
			server.putKV(Integer.toString(i), Integer.toString(i));
		}
		
		InfraMetadata mock = new InfraMetadata();
		mock.addServiceLocation(new ServiceLocation("dummy2", "dummy2", 0));
		server.migrateWithNewMD(mock);
		
		// Keys should be sent but local copies remain.
		assertEquals(keyCount, KVServer.testMigrateCount);
		assertEquals(keyCount, Disk.getAllKeys().size());
		
		// Local copies should be deleted to complete
		// 2-step migration.
		server.removeMigratedKeys(mock);
		assertEquals(0, Disk.getAllKeys().size());
		
		server.resetMigrateResourcesTestOnly();
	}
}
