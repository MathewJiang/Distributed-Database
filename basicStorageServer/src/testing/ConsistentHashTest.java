package testing;

import static org.junit.Assert.*;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.TreeMap;

import org.junit.Before;
import org.junit.Test;

import shared.ConsistentHash;
import shared.InfraMetadata;
import shared.InfraMetadata.ServiceLocation;

public class ConsistentHashTest {
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
			ServiceLocation sl = ch.getSuccessor(server0);
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
	}
	
	
}
