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
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import shared.InfraMetadata.ServiceLocation;

public class ConsistentHash {
	class MD5Comparator implements Comparator<BigInteger> {
   
		@Override
	    public int compare(BigInteger a, BigInteger b) {
			return a.compareTo(b);
		}
	}
	private static Logger logger = Logger.getRootLogger();

	private static TreeMap<BigInteger, ServiceLocation> hashRing 
		= new TreeMap<BigInteger, ServiceLocation>(); //public Map.Entry<K,V> higherEntry(K key)
	public static ReentrantLock hashRingLock = new ReentrantLock();

	
	/***************************************************
	 * Get the Server (address) which contains that key
	 * @param	MD5Key 		Key wants to search for
	 * @return 				The server address in string
	 ***************************************************/
	public static ServiceLocation getServer(String key) {
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
	
	/***************************************************
	 * Add a serverNode into the ring
	 * (in fact it is being inserted into the treeMap)
	 * @param	serverInfo	Server information
	 * @return 	1 on success, 0 on failure
	 ***************************************************/
	public static boolean addServerNode(ServiceLocation serverInfo) {
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
	
	/***************************************************
	 * Remove a serverNode into the ring
	 * @param	serverInfo	Server information
	 * @return 	1 on success, 0 on failure
	 ***************************************************/
	public static boolean removeServerNode(ServiceLocation serverInfo) {
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
	
	/***************************************************
	 * Remove a serverNode into the ring
	 * @param	serverInfo	Server information
	 * @return 	1 on success, 0 on failure
	 ***************************************************/
	public static boolean removeAllServerNodes(ServiceLocation serverInfo) {
		String serverHashString = serverInfo.host + ":" + serverInfo.port.toString();
		BigInteger serverHashMD5 = MD5.getMD5(serverHashString);
		
		hashRingLock.lock();
		try {
			hashRing.remove(serverHashMD5);
		} catch (Exception e) {
			hashRingLock.unlock();
			logger.error("[ConsistentHash.java/removeAllServerNodes]:" +
					"failed at remove all server nodes");
			return false;
		}
		hashRingLock.unlock();
		return true;
	}
	
	
	/*
	 * Testing purposes only
	 * Can be migrated into JUnit tests later on
	 */
	public static void main(String[] args) {
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
		
		ConsistentHash.addServerNode(server1);
		ConsistentHash.addServerNode(server2);
		ConsistentHash.addServerNode(server3);
		
		System.out.println("---round 1----");
		System.out.println(ConsistentHash.getServer("key1").serviceName);
		System.out.println(ConsistentHash.getServer("key2").serviceName);
		System.out.println(ConsistentHash.getServer("key3").serviceName);
		
		ConsistentHash.addServerNode(server4);
		ConsistentHash.addServerNode(server5);
		
		System.out.println("---round 2----");
		System.out.println(ConsistentHash.getServer("key1").serviceName);
		System.out.println(ConsistentHash.getServer("key2").serviceName);
		System.out.println(ConsistentHash.getServer("key3").serviceName);
		
		ConsistentHash.removeServerNode(server4);
		ConsistentHash.removeServerNode(server5);
		
		System.out.println("---round 3----");
		System.out.println(ConsistentHash.getServer("key1").serviceName);
		System.out.println(ConsistentHash.getServer("key2").serviceName);
		System.out.println(ConsistentHash.getServer("key3").serviceName);
    }
}
