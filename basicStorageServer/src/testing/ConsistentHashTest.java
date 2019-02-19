package testing;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import shared.ConsistentHash;
import shared.InfraMetadata.ServiceLocation;

public class ConsistentHashTest {
	ConsistentHash ch;
	
	ServiceLocation server0;
	ServiceLocation server1;
	ServiceLocation server2;
	ServiceLocation server3;
	ServiceLocation server4;
	ServiceLocation server5;
	
	@Before
	public void setUp() throws Exception {
		server0 = new ServiceLocation("server0", "127.0.0.1", 50422);
		server1 = new ServiceLocation("server1", "127.0.0.1", 50000);
		server2 = new ServiceLocation("server2", "127.0.0.1", 50007);
		server3 = new ServiceLocation("server3", "127.0.0.1", 50001);
		server4 = new ServiceLocation("server4", "127.0.0.1", 50006);
		server5 = new ServiceLocation("server5", "127.0.0.1", 50004);
	}

	@Test
	public void testHashRingConstruct() {
		ch = new ConsistentHash();
		fail("Not yet implemented");
	}

	@Test
	public void testAddNode() {
		fail("Not yet implemented");
	}
	
	@Test
	public void testRemoveNode() {
		fail("Not yet implemented");
	}
	
	@Test
	public void testRemoveAll() {
		fail("Not yet implemented");
	}
	
	@Test
	public void testGetSuccessor() {
		fail("Not yet implemented");
	}
	
	@Test
	public void testGetPredecessor() {
		fail("Not yet implemented");
	}
}
