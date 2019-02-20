package testing;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;

import shared.messages.CommMessage;
import shared.messages.KVMessage.StatusType;
import shared.metadata.InfraMetadata;
import shared.metadata.ServiceLocation;
import client.KVStore;
import app_kvECS.ECSClient;
import app_kvServer.KVServer;

public class M1Test extends TestCase {

	ECSClient ecsClient;
	KVStore client;
	KVServer server;
	InfraMetadata md;
	
	@Before
	public void setUp() throws Exception {
		ecsClient = new ECSClient();
		ecsClient.initECS();
		
		md = new InfraMetadata();
		
		ServiceLocation ecsSl = new ServiceLocation("ecs", "127.0.0.1", 39678);
		ArrayList<ServiceLocation> sls = new ArrayList<ServiceLocation>();
		sls.add(new ServiceLocation("server_0", "127.0.0.1", 5000));
		
		md.setServerLocations(sls);
		md.setEcsLocation(ecsSl);
	}

	@Test
	public void testClient() {
		try {
			client = new KVStore();
			client.resetClusterHash(md);
			
			server = new KVServer(5000, 0, "None");
			server.setClusterMD(md);
			server.setServerInfo(new ServiceLocation("server_0", "127.0.0.1", 5000));
			server.start();
			
			Thread.sleep(10);
			server.setSuspended(false);
			
			assertTrue(client.put("key1", "value1").getStatus().equals(StatusType.PUT_SUCCESS));
			assertTrue(client.get("key1").getStatus().equals(StatusType.GET_SUCCESS));
		} catch (IOException e){
			fail("[testM1]IOException");
		} catch (InterruptedException e) {
			fail("[testM1]InterrupedException");
		} catch (Exception e) {
			fail("[testM1]Exception");
		}
	}
	
	@Test
	public void testServer() {
		try {
			client = new KVStore();
			client.resetClusterHash(md);
			
			server = new KVServer(5000, 3, "FIFO");
			server.setClusterMD(md);
			server.setServerInfo(new ServiceLocation("server_0", "127.0.0.1", 5000));
			server.start();
			
			Thread.sleep(10);
			server.setSuspended(false);
			
			assertTrue(client.put("key1", null).getStatus().equals(StatusType.DELETE_SUCCESS));
			assertTrue(client.get("key1").getStatus().equals(StatusType.GET_ERROR));
		} catch (IOException e){
			fail("[testM1]IOException");
		} catch (InterruptedException e) {
			fail("[testM1]InterrupedException");
		} catch (Exception e) {
			fail("[testM1]Exception");
		}
	}
	

}
