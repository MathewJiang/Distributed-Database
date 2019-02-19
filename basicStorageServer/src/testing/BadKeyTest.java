package testing;


import junit.framework.TestCase;

import org.junit.Test;

import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import client.KVStore;

public class BadKeyTest extends TestCase {
//	private KVStore kvClient;
//	
//	public void setUp() {
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
//	@Test
//	public void testEmptyKey() {
//		KVMessage response = null;
//		Exception ex = null;
//
//		try {
//			response = kvClient.put("", "value with an empty key");
//		} catch (Exception e) {
//			ex = e;
//		}
//
//		assertTrue(ex == null && response.getStatus() == StatusType.PUT_ERROR);
//	}
//
//	@Test
//	public void testKeyWithSpace() {
//		KVMessage response = null;
//		Exception ex = null;
//
//		try {
//			response = kvClient.put("key with space", "value with spaced key");
//		} catch (Exception e) {
//			ex = e;
//		}
//
//		assertTrue(ex == null && response.getStatus() == StatusType.PUT_ERROR);
//	}
//	
//	@Test
//	public void testKeyLenOverflow() {
//		KVMessage response = null;
//		Exception ex = null;
//
//		try {
//			response = kvClient.put("123456789012345678901234567890", "value with key length overflow");
//		} catch (Exception e) {
//			ex = e;
//		}
//
//		assertTrue(ex == null && response.getStatus() == StatusType.PUT_ERROR);
//	}
	
	
}
