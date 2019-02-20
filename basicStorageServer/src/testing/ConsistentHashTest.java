package testing;

import static org.junit.Assert.*;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import shared.ConsistentHash;
import shared.metadata.InfraMetadata;
import shared.metadata.ServiceLocation;

public class ConsistentHashTest extends TestCase {
	ConsistentHash ch;
	
	ServiceLocation ecs;
	ServiceLocation server0;
	ServiceLocation server1;
	ServiceLocation server2;
	ServiceLocation server3;
	ServiceLocation server4;
	ServiceLocation server5;
	
	
	@Before
	public void setUp() throws Exception {
		ecs = new ServiceLocation("ecs", "127.0.0.1", 39678);
		server0 = new ServiceLocation("server_0", "127.0.0.1", 50422);
		server1 = new ServiceLocation("server_1", "127.0.0.1", 50000);
		server2 = new ServiceLocation("server_2", "127.0.0.1", 50007);
		server3 = new ServiceLocation("server_3", "127.0.0.1", 50001);
		server4 = new ServiceLocation("server_4", "127.0.0.1", 50006);
		server5 = new ServiceLocation("server_5", "127.0.0.1", 50004);
	}

	@Test
	public void testHashRingConstruct() {
		ch = new ConsistentHash();
		TreeMap<BigInteger, ServiceLocation> hashRing = ch.getHashRing();
		assertTrue(hashRing == null || hashRing.size() == 0);
		
		InfraMetadata md = new InfraMetadata();
		md.setEcsLocation(ecs);
		ArrayList<ServiceLocation> serverLocations = new ArrayList<ServiceLocation>();
		serverLocations.add(server0);
		serverLocations.add(server1);
		serverLocations.add(server2);
		serverLocations.add(server3);
		serverLocations.add(server4);
		serverLocations.add(server5);
		md.setServerLocations(serverLocations);
		
		ch.addNodesFromInfraMD(md);
		assertTrue(ch.getHashRing().size() == 6);
	}

	@Test
	public void testAddNode() {
		ch = new ConsistentHash();
		
		ch.addServerNode(server0);
		ch.addServerNode(server1);
		ch.addServerNode(server2);
		ch.addServerNode(server3);
		ch.addServerNode(server4);
		ch.addServerNode(server5);
		assertTrue(ch.getHashRing().size() == 6);
		
		ch.addServerNode(server0);
		assertTrue(ch.getHashRing().size() == 6);
	}
	
	@Test
	public void testRemoveNode() {
		ch = new ConsistentHash();
		
		ch.addServerNode(server0);
		ch.addServerNode(server1);
		ch.addServerNode(server2);
		ch.addServerNode(server3);
		ch.addServerNode(server4);
		ch.addServerNode(server5);
		assertTrue(ch.getHashRing().size() == 6);
		
		ch.removeServerNode(server0);
		assertTrue(ch.getHashRing().size() == 5);
		ch.removeServerNode(server1);
		assertTrue(ch.getHashRing().size() == 4);
	}
	
	@Test
	public void testRemoveAll() {
		ch = new ConsistentHash();
		
		ch.addServerNode(server0);
		ch.addServerNode(server1);
		ch.addServerNode(server2);
		ch.addServerNode(server3);
		ch.addServerNode(server4);
		ch.addServerNode(server5);
		assertTrue(ch.getHashRing().size() == 6);
		
		ch.removeAllServerNodes();
		assertTrue(ch.getHashRing().size() == 0);
	}
	
	@Test
	public void testGetRange() {
		ch = new ConsistentHash();
		
		ch.addServerNode(server0);
		ch.addServerNode(server1);
		ch.addServerNode(server2);
		ch.addServerNode(server3);
		ch.addServerNode(server4);
		ch.addServerNode(server5);
		
		try {
			String[] server0RangeString = ch.getHashRange(server0);
			BigInteger[] server0RangeBigInteger = ch.getHashRangeInteger(server0);
			
			assertTrue(server0RangeString[0].equals("0xDCEE0277EB13B76434E8DCD31A38770A"));
			assertTrue(server0RangeString[1].equals("0x00DB55462813DF703931CA46BF319D9B"));
			
			assertTrue(server0RangeBigInteger[0].compareTo(new BigInteger("293665975790736140455366914418740328202")) == 0);
			assertTrue(server0RangeBigInteger[1].compareTo(new BigInteger("1138842575210238782446640189423066523")) == 0);
		} catch (Exception e) {
			e.printStackTrace();
			fail("[0]testGetRange failed!");
		}
		
		try {
			String[] server5RangeString = ch.getHashRange(server5);
			BigInteger[] server5RangeBigInteger = ch.getHashRangeInteger(server5);
			
			assertTrue(server5RangeString[0].equals("0x8313085F59DFED5215AFE928FC846356"));
			assertTrue(server5RangeString[1].equals("0xDA850509FC3B88A612B0BCAD7A37963B"));
			
			assertTrue(server5RangeBigInteger[0].compareTo(new BigInteger("174227690901903286190742218822195110742")) == 0);
			assertTrue(server5RangeBigInteger[1].compareTo(new BigInteger("290462380766460467658039843920390100539")) == 0);
		} catch (Exception e) {
			e.printStackTrace();
			fail("[5]testGetRange failed!");
		} 
		
	}
	
	@Test
	public void testGetSuccessor() {
		ch = new ConsistentHash();
		
		ch.addServerNode(server0);
		ch.addServerNode(server1);
		ch.addServerNode(server2);
		ch.addServerNode(server3);
		ch.addServerNode(server4);
		ch.addServerNode(server5);
		
		try {
			ServiceLocation sl0Successor = ch.getSuccessor(server0);
			assertTrue(sl0Successor.equals(server2));
			
			ServiceLocation sl1Successor = ch.getSuccessor(server1);
			assertTrue(sl1Successor.equals(server4));
		} catch (Exception e) {
			e.printStackTrace();
			fail("[0]getSuccessor failed!");
		}
	}
	
	@Test
	public void testGetPredecessor() {
		ch = new ConsistentHash();
		
		ch.addServerNode(server0);
		ch.addServerNode(server1);
		ch.addServerNode(server2);
		ch.addServerNode(server3);
		ch.addServerNode(server4);
		ch.addServerNode(server5);
		
		try {
			ServiceLocation sl0Predecessor = ch.getPredeccessor(server0);
			assertTrue(sl0Predecessor.equals(server3));
			
			ServiceLocation sl1Predecessor = ch.getPredeccessor(server1);
			assertTrue(sl1Predecessor.equals(server2));
		} catch (Exception e) {
			e.printStackTrace();
			fail("[0]getPredecessor failed!");
		}
	}
	
	@Test
	public void testGetServer() {
		ch = new ConsistentHash();
		
		ch.addServerNode(server0);
		ch.addServerNode(server1);
		ch.addServerNode(server2);
		ch.addServerNode(server3);
		ch.addServerNode(server4);
		ch.addServerNode(server5);
		
		assertTrue(ch.getServer("key1").equals(server5));
		assertTrue(ch.getServer("key2").equals(server4));
		assertTrue(ch.getServer("key3").equals(server4));
		
		ch.removeServerNode(server2);
		ch.removeServerNode(server5);
		
		assertTrue(ch.getServer("key1").equals(server3));
		assertTrue(ch.getServer("key2").equals(server4));
		assertTrue(ch.getServer("key3").equals(server4));
		
	}
	
	@Test
	public void testGetServerSingle() {
		ch = new ConsistentHash();
		
		ch.addServerNode(server0);
		
		assertTrue(ch.getServer("key1").equals(server0));
		assertTrue(ch.getServer("key2").equals(server0));
		assertTrue(ch.getServer("key3").equals(server0));
	}
	
	@After
	public void cleanup() {
		if (ch != null) {
			ch.getHashRing().clear();
		}
	}
	
}
