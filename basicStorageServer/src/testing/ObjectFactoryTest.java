package testing;


import org.junit.Before;
import org.junit.Test;

import client.KVStore;
import junit.framework.TestCase;

import app_kvClient.KVClient;
import app_kvServer.KVServer;
import shared.ObjectFactory;

public class ObjectFactoryTest extends TestCase {

	private KVClient client = null;
	private KVServer server = null;
	
	@Before
	protected void setUp() throws Exception {
		client = (KVClient) ObjectFactory.createKVClientObject();
		server = (KVServer) ObjectFactory.createKVServerObject(0, 5, "FIFO");
		((KVServer)server).start();
	}

	@Test
	public void testBasicPUTGET() {
		try {
			client.newConnection("localhost", server.getPort());
		} catch (Exception e1) {
			fail("[ObjectFactoryTest]setUp(): Error! Unable to connect with the server!");
		}
		assertNotNull(client);
		assertNotNull(server);
		
		try {
			server.putKV("test", "test string");
		} catch (Exception e) {
			fail("[ObjectFactoryTest]basicPUTGETTest: putKV failed");
		}
		
		String result = null;
		KVStore kvs = null;
		try {
			result = server.getKV("test");
			kvs = (KVStore)(client.getStore());
		} catch (Exception e) {
			fail("[ObjectFactoryTest]basicPUTGETTest: getKV failed");
		}
		
		assertEquals(result, "test string");
	}
	
	@Test
	public void connectionTest() {
		fail("Not yet implemented");
	}
	
}
