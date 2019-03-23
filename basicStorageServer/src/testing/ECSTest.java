package testing;

import junit.framework.TestCase;

import org.junit.Before;
import org.junit.Test;
import ecs.ECSNode;

import shared.metadata.InfraMetadata;

import app_kvECS.ECSClient;

public class ECSTest extends TestCase {

	ECSClient ecsClient;
	
	@Before
	public void setUp() throws Exception {
		ecsClient = new ECSClient();
		ecsClient.initECS();
	}

	@Test(timeout = 1000)
	public void testConnection() {
		ecsClient.getECS().reset();
		ecsClient.setupNodes(1, "None", 0);
		
		InfraMetadata md = ecsClient.getECS().getMD();
		assertTrue(md.getServerLocations().size() == 1);
		
		ecsClient.shutdown();
		ecsClient.getECS().reset();
	}
	
	
	@Test(timeout = 1000)
	public void testAddNode(){
		ecsClient.getECS().reset();
		ecsClient.getECS().init();
		ecsClient.setupNodes(1, "None", 0);
		
		ecsClient.addNode("None", 0);
		
		InfraMetadata md = ecsClient.getECS().getMD();
		assertTrue(md.getServerLocations().size() == 2);
		
		ecsClient.shutdown();
		ecsClient.getECS().reset();
	}
	
	@Test(timeout = 1000)
	public void testRemoveNode(){
		ecsClient.getECS().reset();
		ecsClient.getECS().init();
		ecsClient.setupNodes(2, "None", 0);
		
		ecsClient.removeNode("server_0");
		
		InfraMetadata md = ecsClient.getECS().getMD();
		assertTrue(md.getServerLocations().size() == 1);
		
		ecsClient.shutdown();
		ecsClient.getECS().reset();
	}

	@Test(timeout = 1000)
	public void testGetNodesByKey(){
		ecsClient.getECS().reset();
		ecsClient.setupNodes(1, "None", 0);
		
		ecsClient.getECS().printHash();
		ECSNode ecsNode = (ECSNode) ecsClient.getNodeByKey("key1");
		assertTrue(ecsNode.getNodeName().equals("server_0"));
		
		
		ecsClient.shutdown();
		ecsClient.getECS().reset();
	}
	
	@Test(timeout = 1000)
	public void testAwaitNodes() {
		ecsClient.getECS().reset();
		ecsClient.setupNodes(1, "None", 0);
		
		try {
			boolean res = ecsClient.awaitNodes(1, 1000);
			if (res == false) {
				fail("[ECSTest]failed at testAwaitNodes: res is false!");
			}
		} catch (Exception e) {
			fail("[ECSTest]failed at testAwaitNodes");
			e.printStackTrace();
		}
		ecsClient.shutdown();
		ecsClient.getECS().reset();
	}
	
//	@Test
//	public void testTemplate() {
//		ecsClient.getECS().reset();
//		ecsClient.setupNodes(1, "None", 0);
//		
//		ecsClient.shutdown();
//	}


}
