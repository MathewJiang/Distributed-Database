/***************************************************************
 * Consistent Hash:
 * 
 * (In reality, we don't necessarily need the ring-like structure)
 * Arrange the MD5 value of the object and serverID
 * in a ring-like structure
 * 
 * 
 * The data/key belongs to the 
 * closest server closkwise within the ring
 * 
 ***************************************************************/
package shared;

import java.math.BigInteger;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import shared.InfraMetadata.ServiceLocation;

public class ConsistentHash {
	private static Logger logger = Logger.getRootLogger();

	private TreeMap<BigInteger, ServiceLocation> hashRing 
		= new TreeMap<BigInteger, ServiceLocation>();
	public static ReentrantLock hashRingLock = new ReentrantLock();

	public void addNodesFromInfraMD(InfraMetadata md) {
		for (ServiceLocation srv : md.getServerLocations()) {
			addServerNode(srv);
		}
	}
	
	/*****************************************************************************
	 * Get the Server (address) which contains that key
	 * @param	MD5Key 		Key wants to search for
	 * @return 				The server address in string
	 *****************************************************************************/
	public ServiceLocation getServer(String key) {
		BigInteger MD5Key = MD5.getMD5(key);
		
		hashRingLock.lock();
		if (hashRing.size() == 0) {
			hashRingLock.unlock();
			return null;
		}
		
		Entry<BigInteger, ServiceLocation>serverEntry = hashRing.higherEntry(MD5Key);
		hashRingLock.unlock();
		
		if (serverEntry == null) {
			//if unfound,
			//meaning we need to go to the first elem in the ring
			hashRingLock.lock();
			serverEntry = hashRing.firstEntry();
			hashRingLock.unlock();
		}
		
		return serverEntry.getValue();
	}
	
	/*****************************************************************************
	 * Add a serverNode into the ring
	 * (in fact it is being inserted into the treeMap)
	 * @param	serverInfo	Server information
	 * @return 	1 on success, 0 on failure
	 *****************************************************************************/
	public boolean addServerNode(ServiceLocation serverInfo) {
		String serverHashString = serverInfo.host + ":" + serverInfo.port.toString();
		BigInteger serverHashMD5 = MD5.getMD5(serverHashString);
		
		hashRingLock.lock();
		try {
			hashRing.put(serverHashMD5, serverInfo);
		} catch (Exception e) {
			hashRingLock.unlock();
			logger.error("[ConsistentHash.java/addServerNode]:" +
					"failed at adding new server node");
			return false;
		}
		hashRingLock.unlock();
		return true;
	}
	
	/*****************************************************************************
	 * Remove a serverNode into the ring
	 * @param	serverInfo	Server information
	 * @return 	1 on success, 0 on failure
	 *****************************************************************************/
	public boolean removeServerNode(ServiceLocation serverInfo) {
		String serverHashString = serverInfo.host + ":" + serverInfo.port.toString();
		BigInteger serverHashMD5 = MD5.getMD5(serverHashString);
		
		hashRingLock.lock();
		try {
			hashRing.remove(serverHashMD5);
		} catch (Exception e) {
			hashRingLock.unlock();
			logger.error("[ConsistentHash.java/removeServerNode]:" +
					"failed at removing new server node");
			return false;
		}
		hashRingLock.unlock();
		return true;
	}
	
	/*****************************************************************************
	 * Remove a serverNode into the ring
	 * @param	serverInfo	Server information
	 * @return 	1 on success, 0 on failure
	 *****************************************************************************/
	public boolean removeAllServerNodes() {
		
		hashRingLock.lock();
		try {
			hashRing.clear();
		} catch (Exception e) {
			hashRingLock.unlock();
			logger.error("[ConsistentHash.java/removeAllServerNodes]:" +
					"failed at remove all server nodes");
			return false;
		}
		hashRingLock.unlock();
		return true;
	}
	
	/*****************************************************************************
	 * Get hash range given a server
	 * @param	serverInfo	Server information
	 * @return 	hashRange	String[0] = low
	 * 						String[1] = high
	 * 
	 * @throws	Exception	if the hashRing has not been constructed
	 * Note: if low > high, meaning it loops around the ring
	 *****************************************************************************/
	public String[] getHashRange(ServiceLocation serverInfo) throws Exception {
		if (serverInfo == null) {
			throw new Exception("[ConsistentHash.java/getHashRange]ServerInfo is null!");
		}
		
		String[] hashRange = new String[2];
		String serverHashString = serverInfo.host + ":" + serverInfo.port.toString();
		BigInteger MD5ServerInfo = MD5.getMD5(serverHashString);
		
		// TODO: if only one element in hashRing
		// TODO: the serverMD5 is 0x0000
		hashRingLock.lock();
		if (hashRing.size() == 0) {
			hashRingLock.unlock();
			throw new Exception("[ConsistentHash.java/getHashRange]HashRing has not been constructed!");
		} else if (hashRing.size() == 1) {
			hashRange[0] = String.format("0x%32X", MD5ServerInfo.add(BigInteger.ONE));
			if (MD5ServerInfo.compareTo(BigInteger.ZERO) == 0) {
				hashRange[1] = String.format("0x%32X", BigInteger.ONE.shiftLeft(32).subtract(BigInteger.ONE));
			} else {
				hashRange[1] = String.format("0x%32X", MD5ServerInfo.subtract(BigInteger.ONE));
			}
		} else {
			Entry<BigInteger, ServiceLocation>predecessorEntry = hashRing.lowerEntry(MD5ServerInfo);
			if (predecessorEntry == null) {
				//if the server is the first element
				predecessorEntry = hashRing.lastEntry();
			}
			hashRange[0] = String.format("0x%32X", (predecessorEntry.getKey().add(BigInteger.ONE)));
			
			if (MD5ServerInfo.compareTo(BigInteger.ZERO) == 0) {
				hashRange[1] = String.format("0x%32X", BigInteger.ONE.shiftLeft(32).subtract(BigInteger.ONE));
			} else {
				hashRange[1] = String.format("0x%32X", MD5ServerInfo.subtract(BigInteger.ONE));
			}
		}
		
		hashRingLock.unlock();
		return hashRange;
	}
	
	/*
	 * Testing purposes only
	 * Can be migrated into JUnit tests later on
	 */
	public static void main(String[] args) {
		ConsistentHash ch = new ConsistentHash();
		
		ServiceLocation server1 = new ServiceLocation("server1", "127.0.0.1", 50000);
		ServiceLocation server2 = new ServiceLocation("server2", "127.0.0.1", 50001);
		ServiceLocation server3 = new ServiceLocation("server3", "127.0.0.1", 50002);
		ServiceLocation server4 = new ServiceLocation("server4", "127.0.0.1", 50003);
		ServiceLocation server5 = new ServiceLocation("server5", "127.0.0.1", 50004);
		
		System.out.println("server1 MD5: " + String.format("0x%32X", MD5.getMD5("127.0.0.1:50000")));
		System.out.println("server2 MD5: " + String.format("0x%32X", MD5.getMD5("127.0.0.1:50001")));
		System.out.println("server3 MD5: " + String.format("0x%32X", MD5.getMD5("127.0.0.1:50002")));
		System.out.println("server4 MD5: " + String.format("0x%32X", MD5.getMD5("127.0.0.1:50003")));
		System.out.println("server5 MD5: " + String.format("0x%32X", MD5.getMD5("127.0.0.1:50004")));
		
		System.out.println("key1: " + String.format("0x%32X", MD5.getMD5("key1")));
		System.out.println("key2: " + String.format("0x%32X", MD5.getMD5("key2")));
		System.out.println("key3: " + String.format("0x%32X", MD5.getMD5("key3")));
		
		ch.addServerNode(server1);
		ch.addServerNode(server2);
		ch.addServerNode(server3);
		
		String[] server1HashRange = null;
		try {
			server1HashRange = ch.getHashRange(server1);
		} catch (Exception e) {
			System.out.println("gg!");
			e.printStackTrace();
		}
		
		System.out.println("---server1 range from: " + server1HashRange[0]);
		System.out.println("to: " + server1HashRange[1] + "---");
		
		System.out.println("---round 1----");
		System.out.println(ch.getServer("key1").serviceName);
		System.out.println(ch.getServer("key2").serviceName);
		System.out.println(ch.getServer("key3").serviceName);
		
		ch.addServerNode(server4);
		ch.addServerNode(server5);
		
		System.out.println("---round 2----");
		System.out.println(ch.getServer("key1").serviceName);
		System.out.println(ch.getServer("key2").serviceName);
		System.out.println(ch.getServer("key3").serviceName);
		
		ch.removeServerNode(server2);
		ch.removeServerNode(server5);
		
		System.out.println("---round 3----");
		System.out.println(ch.getServer("key1").serviceName);
		System.out.println(ch.getServer("key2").serviceName);
		System.out.println(ch.getServer("key3").serviceName);
		
		ch.removeAllServerNodes();
		System.out.println("---round 4----");
		if (ch.getServer("key1") != null) {
			System.out.println("gg!");
		}
		if (ch.getServer("key2") != null) {
			System.out.println("gg!");
		}
		if (ch.getServer("key3") != null) {
			System.out.println("gg!");
		}
		
    }
}
