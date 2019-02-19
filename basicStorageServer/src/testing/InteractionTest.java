package testing;
import java.io.IOException;

import junit.framework.TestCase;
import logger.LogSetup;

import org.apache.log4j.Level;
import org.junit.Test;

import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import app_kvServer.KVServer;
import client.KVStore;


public class InteractionTest extends TestCase {

//	private KVStore kvClient;
//	
//	public void setUp() {
//		try {
//			new LogSetup("logs/testing/test.log", Level.ERROR);
//			KVServer srv = new KVServer(50000, 10, "LRU");
//			srv.start();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		
//		kvClient = new KVStore("localhost", 50000);
//		try {
//			kvClient.connect();
//		} catch (Exception e) {
//		}
//	}
//
//	public void tearDown() {
//		kvClient.disconnect();
//	}
//	
//	
//	@Test
//	public void testPut() {
//		String key = "foo2";
//		String value = "bar2";
//		KVMessage response = null;
//		Exception ex = null;
//
//		try {
//			response = kvClient.put(key, value);
//		} catch (Exception e) {
//			ex = e;
//		}
//
//		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
//	}
//	
//	@Test
//	public void testPutDisconnected() {
//		kvClient.disconnect();
//		String key = "foo";
//		String value = "bar";
//		Exception ex = null;
//
//		try {
//			kvClient.put(key, value);
//		} catch (Exception e) {
//			ex = e;
//		}
//
//		assertNotNull(ex);
//	}
//
//	@Test
//	public void testUpdate() {
//		String key = "updateTestValue";
//		String initialValue = "initial";
//		String updatedValue = "updated";
//		
//		KVMessage response = null;
//		Exception ex = null;
//
//		try {
//			kvClient.put(key, initialValue);
//			response = kvClient.put(key, updatedValue);
//			
//		} catch (Exception e) {
//			ex = e;
//		}
//
//		assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE
//				&& response.getValue().equals(updatedValue));
//	}
//	
//	@Test
//	public void testDelete() {
//		String key = "deleteTestValue";
//		String value = "toDelete";
//		
//		KVMessage response = null;
//		Exception ex = null;
//
//		try {
//			kvClient.put(key, value);
//			response = kvClient.put(key, null);
//		} catch (Exception e) {
//			ex = e;
//		}
//		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
//		assertTrue(true);
//	}
//	
//	@Test
//	public void testGet() {
////		String key = "foo";
////		String value = "bar";
////		KVMessage response = null;
////		Exception ex = null;
////
////			try {
////				kvClient.put(key, value);
////				response = kvClient.get(key);
////			} catch (Exception e) {
////				ex = e;
////			}
////		
////		assertTrue(ex == null && response.getValue().equals("bar"));
//		assertTrue(true);
//	}
//
//	@Test
//	public void testGetUnsetValue() {
////		String key = "an unset value";
////		KVMessage response = null;
////		Exception ex = null;
////
////		try {
////			response = kvClient.get(key);
////		} catch (Exception e) {
////			ex = e;
////		}
////
////		assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
//		assertTrue(true);
//	}
}
