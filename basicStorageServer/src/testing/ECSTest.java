package testing;

import static org.junit.Assert.*;
import junit.framework.TestCase;

import org.junit.Before;
import org.junit.Test;

import ecs.ECSNode;

import shared.InfraMetadata;

import app_kvECS.ECSClient;

public class ECSTest extends TestCase {

	ECSClient ecsClient;
	
	@Before
	public void setUp() throws Exception {
		ecsClient = new ECSClient();
		ecsClient.initECS();
	}

	@Test
	public void testConnection() {
		ecsClient.getECS().reset();
		ecsClient.setupNodes(1, "None", 0);
		
		InfraMetadata md = ecsClient.getECS().getMD();
		assertTrue(md.getServerLocations().size() == 1);
		
		ecsClient.shutdown();
	}
	
	
	@Test
	public void testAddNode(){
		ecsClient.getECS().reset();
		ecsClient.getECS().init();
		ecsClient.setupNodes(1, "None", 0);
		
		ecsClient.addNode("None", 0);
		
		InfraMetadata md = ecsClient.getECS().getMD();
		assertTrue(md.getServerLocations().size() == 2);
		
		ecsClient.shutdown();
	}
	
	@Test
	public void testRemoveNode(){
		ecsClient.getECS().reset();
		ecsClient.getECS().init();
		ecsClient.setupNodes(2, "None", 0);
		
		ecsClient.removeNode("server_0");
		
		InfraMetadata md = ecsClient.getECS().getMD();
		assertTrue(md.getServerLocations().size() == 1);
		
		ecsClient.shutdown();
	}

	@Test
	public void testGetNodesByKey(){
		ecsClient.getECS().reset();
		ecsClient.setupNodes(1, "None", 0);
		
		ecsClient.getECS().printHash();
		ECSNode ecsNode = (ECSNode) ecsClient.getNodeByKey("key1");
		assertTrue(ecsNode.getNodeName().equals("server_0"));
		
		ecsClient.shutdown();
	}
}
