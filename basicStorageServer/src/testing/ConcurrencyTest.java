package testing;


import junit.framework.TestCase;

import org.junit.Test;

import shared.ObjectFactory;
import app_kvServer.IKVServer;
import app_kvServer.KVServer;

public class ConcurrencyTest extends TestCase {	
	private IKVServer server1 = null;
	private IKVServer server2 = null;
	private IKVServer server3 = null;
	

	@Test
	public void testMultiPUTFIFO() {
		server1 = ObjectFactory.createKVServerObject(1025, 5, "FIFO");
		((KVServer)server1).start();
		
		assertNotNull(server1);
		server1.clearStorage();
		
		for (int a = 0; a < 100; a++) {
		try {
			
			Thread threadPool[] = new Thread[30];
			threadPool[0] = new Thread(new Runnable() {

				@Override
				public void run() {
					String key = "key0";
					String value = "value0";
					
					try {
						server1.putKV(key, value);
					} catch (Exception e) {
						fail("[MultiClientTest]testMultiPUT, FIFO");
					}
				}
				
			});
			
			
			threadPool[1] = new Thread(new Runnable() {

				@Override
				public void run() {
					String key = "key1";
					String value = "value1";
					
					try {
						server1.putKV(key, value);
					} catch (Exception e) {
						fail("[MultiClientTest]testMultiPUT, FIFO");
					}
				}
				
			});
			
			threadPool[2] = new Thread(new Runnable() {

				@Override
				public void run() {
					String key = "key2";
					String value = "value2";
					
					try {
						server1.putKV(key, value);
					} catch (Exception e) {
						fail("[MultiClientTest]testMultiPUT, FIFO");
					}
				}
				
			});
			
			threadPool[3] = new Thread(new Runnable() {

				@Override
				public void run() {
					String key = "key3";
					String value = "value3";
					
					try {
						server1.putKV(key, value);
					} catch (Exception e) {
						fail("[MultiClientTest]testMultiPUT, FIFO");
					}
				}
				
			});
			
			threadPool[4] = new Thread(new Runnable() {

				@Override
				public void run() {
					String key = "key4";
					String value = "value4";
					
					try {
						server1.putKV(key, value);
					} catch (Exception e) {
						fail("[MultiClientTest]testMultiPUT, FIFO");
					}
				}
				
			});
			
			threadPool[5] = new Thread(new Runnable() {

				@Override
				public void run() {
					String key = "key5";
					String value = "value5";
					
					try {
						server1.putKV(key, value);
					} catch (Exception e) {
						fail("[MultiClientTest]testMultiPUT, FIFO");
					}
				}
				
			});
			
			threadPool[6] = new Thread(new Runnable() {

				@Override
				public void run() {
					String key = "key6";
					String value = "value6";
					
					try {
						server1.putKV(key, value);
					} catch (Exception e) {
						fail("[MultiClientTest]testMultiPUT, FIFO");
					}
				}
				
			});
			
			threadPool[7] = new Thread(new Runnable() {

				@Override
				public void run() {
					String key = "key7";
					String value = "value7";
					
					try {
						server1.putKV(key, value);
					} catch (Exception e) {
						fail("[MultiClientTest]testMultiPUT, FIFO");
					}
				}
				
			});
			
			threadPool[8] = new Thread(new Runnable() {

				@Override
				public void run() {
					String key = "key8";
					String value = "value8";
					
					try {
						server1.putKV(key, value);
					} catch (Exception e) {
						fail("[MultiClientTest]testMultiPUT, FIFO");
					}
				}
				
			});
			
			threadPool[9] = new Thread(new Runnable() {

				@Override
				public void run() {
					String key = "key9";
					String value = "value9";
					
					try {
						server1.putKV(key, value);
					} catch (Exception e) {
						fail("[MultiClientTest]testMultiPUT, FIFO");
					}
				}
				
			});
			
			threadPool[10] = new Thread(new Runnable() {

				@Override
				public void run() {
					String key = "key10";
					String value = "value10";
					
					try {
						server1.putKV(key, value);
					} catch (Exception e) {
						fail("[MultiClientTest]testMultiPUT, FIFO");
					}
				}
			});
			
			threadPool[0].start();
			threadPool[1].start();
			threadPool[2].start();
			threadPool[3].start();
			threadPool[4].start();
			threadPool[5].start();
			threadPool[6].start();
			threadPool[7].start();
			threadPool[8].start();
			threadPool[9].start();
			threadPool[10].start();
			
			for (int i = 0; i < 11; i++) {
				threadPool[i].join();
			}
			
			
			String key = null;
			String value = null;
			
			for (int i = 0; i < 11; i++) {
				key = "key" + (new Integer(i)).toString();
				value = "value" + (new Integer(i)).toString();
				
//				System.out.println(value + ": " + server1.getKV(key));
				assertEquals(value, server1.getKV(key));
			}
			
		} catch (Exception e) {
			fail("[MultiClientTest]testMultiPUT: Exception");
		} finally {
//			server1.close();
			server1.clearStorage();
		}
		}
	}
	
	
	@Test
	public void testMultiPUTLRU() {
		server2 = ObjectFactory.createKVServerObject(1026, 5, "LRU");
		((KVServer)server2).start();
		
		assertNotNull(server2);
		server2.clearStorage();
		
		try {
			
			Thread threadPool[] = new Thread[30];
			threadPool[0] = new Thread(new Runnable() {

				@Override
				public void run() {
					String key = "key0";
					String value = "value0";
					
					try {
						server2.putKV(key, value);
					} catch (Exception e) {
						fail("[MultiClientTest]testMultiPUT, LRU");
					}
				}
				
			});
			
			
			threadPool[1] = new Thread(new Runnable() {

				@Override
				public void run() {
					String key = "key1";
					String value = "value1";
					
					try {
						server2.putKV(key, value);
					} catch (Exception e) {
						fail("[MultiClientTest]testMultiPUT, LRU");
					}
				}
				
			});
			
			threadPool[2] = new Thread(new Runnable() {

				@Override
				public void run() {
					String key = "key2";
					String value = "value2";
					
					try {
						server2.putKV(key, value);
					} catch (Exception e) {
						fail("[MultiClientTest]testMultiPUT, LRU");
					}
				}
				
			});
			
			threadPool[3] = new Thread(new Runnable() {

				@Override
				public void run() {
					String key = "key3";
					String value = "value3";
					
					try {
						server2.putKV(key, value);
					} catch (Exception e) {
						fail("[MultiClientTest]testMultiPUT, LRU");
					}
				}
				
			});
			
			threadPool[4] = new Thread(new Runnable() {

				@Override
				public void run() {
					String key = "key4";
					String value = "value4";
					
					try {
						server2.putKV(key, value);
					} catch (Exception e) {
						fail("[MultiClientTest]testMultiPUT, LRU");
					}
				}
				
			});
			
			threadPool[5] = new Thread(new Runnable() {

				@Override
				public void run() {
					String key = "key5";
					String value = "value5";
					
					try {
						server2.putKV(key, value);
					} catch (Exception e) {
						fail("[MultiClientTest]testMultiPUT, LRU");
					}
				}
				
			});
			
			threadPool[6] = new Thread(new Runnable() {

				@Override
				public void run() {
					String key = "key6";
					String value = "value6";
					
					try {
						server2.putKV(key, value);
					} catch (Exception e) {
						fail("[MultiClientTest]testMultiPUT, LRU");
					}
				}
				
			});
			
			threadPool[7] = new Thread(new Runnable() {

				@Override
				public void run() {
					String key = "key7";
					String value = "value7";
					
					try {
						server2.putKV(key, value);
					} catch (Exception e) {
						fail("[MultiClientTest]testMultiPUT, LRU");
					}
				}
				
			});
			
			threadPool[8] = new Thread(new Runnable() {

				@Override
				public void run() {
					String key = "key8";
					String value = "value8";
					
					try {
						server2.putKV(key, value);
					} catch (Exception e) {
						fail("[MultiClientTest]testMultiPUT, LRU");
					}
				}
				
			});
			
			threadPool[9] = new Thread(new Runnable() {

				@Override
				public void run() {
					String key = "key9";
					String value = "value9";
					
					try {
						server2.putKV(key, value);
					} catch (Exception e) {
						fail("[MultiClientTest]testMultiPUT, LRU");
					}
				}
				
			});
			
			threadPool[10] = new Thread(new Runnable() {

				@Override
				public void run() {
					String key = "key10";
					String value = "value10";
					
					try {
						server2.putKV(key, value);
					} catch (Exception e) {
						fail("[MultiClientTest]testMultiPUT, LRU");
					}
				}
				
			});
			
			threadPool[0].start();
			threadPool[1].start();
			threadPool[2].start();
			threadPool[3].start();
			threadPool[4].start();
			threadPool[5].start();
			threadPool[6].start();
			threadPool[7].start();
			threadPool[8].start();
			threadPool[9].start();
			threadPool[10].start();
			
			for (int i = 0; i < 11; i++) {
				threadPool[i].join();
			}
			
			
			String key = null;
			String value = null;
			for (int i = 0; i < 11; i++) {
				key = "key" + (new Integer(i)).toString();
				value = "value" + (new Integer(i)).toString();
				
				assertEquals(server2.getKV(key), value);
			}
			
		} catch (Exception e) {
			fail("[MultiClientTest]testMultiPUTLRU: Exception");
		} finally {
//			server2.close();
			server2.clearStorage();
		}
		
	}
	
	
	@Test
	public void testMultiPUTLFU() {
		server3 = ObjectFactory.createKVServerObject(1027, 5, "LFU");
		((KVServer)server3).start();
		
		assertNotNull(server3);
		server3.clearStorage();
		
		try {
			
			Thread threadPool[] = new Thread[30];
			threadPool[0] = new Thread(new Runnable() {

				@Override
				public void run() {
					String key = "key0";
					String value = "value0";
					
					try {
						server3.putKV(key, value);
					} catch (Exception e) {
						fail("[MultiClientTest]testMultiPUT, LFU");
					}
				}
				
			});
			
			
			threadPool[1] = new Thread(new Runnable() {

				@Override
				public void run() {
					String key = "key1";
					String value = "value1";
					
					try {
						server3.putKV(key, value);
					} catch (Exception e) {
						fail("[MultiClientTest]testMultiPUT, LFU");
					}
				}
				
			});
			
			threadPool[2] = new Thread(new Runnable() {

				@Override
				public void run() {
					String key = "key2";
					String value = "value2";
					
					try {
						server3.putKV(key, value);
					} catch (Exception e) {
						fail("[MultiClientTest]testMultiPUT, LFU");
					}
				}
				
			});
			
			threadPool[3] = new Thread(new Runnable() {

				@Override
				public void run() {
					String key = "key3";
					String value = "value3";
					
					try {
						server3.putKV(key, value);
					} catch (Exception e) {
						fail("[MultiClientTest]testMultiPUT, LFU");
					}
				}
				
			});
			
			threadPool[4] = new Thread(new Runnable() {

				@Override
				public void run() {
					String key = "key4";
					String value = "value4";
					
					try {
						server3.putKV(key, value);
					} catch (Exception e) {
						fail("[MultiClientTest]testMultiPUT, LFU");
					}
				}
				
			});
			
			threadPool[5] = new Thread(new Runnable() {

				@Override
				public void run() {
					String key = "key5";
					String value = "value5";
					
					try {
						server3.putKV(key, value);
					} catch (Exception e) {
						fail("[MultiClientTest]testMultiPUT, LFU");
					}
				}
				
			});
			
			threadPool[6] = new Thread(new Runnable() {

				@Override
				public void run() {
					String key = "key6";
					String value = "value6";
					
					try {
						server3.putKV(key, value);
					} catch (Exception e) {
						fail("[MultiClientTest]testMultiPUT, LFU");
					}
				}
				
			});
			
			threadPool[7] = new Thread(new Runnable() {

				@Override
				public void run() {
					String key = "key7";
					String value = "value7";
					
					try {
						server3.putKV(key, value);
					} catch (Exception e) {
						fail("[MultiClientTest]testMultiPUT, LFU");
					}
				}
				
			});
			
			threadPool[8] = new Thread(new Runnable() {

				@Override
				public void run() {
					String key = "key8";
					String value = "value8";
					
					try {
						server3.putKV(key, value);
					} catch (Exception e) {
						fail("[MultiClientTest]testMultiPUT, LFU");
					}
				}
				
			});
			
			threadPool[9] = new Thread(new Runnable() {

				@Override
				public void run() {
					String key = "key9";
					String value = "value9";
					
					try {
						server3.putKV(key, value);
					} catch (Exception e) {
						fail("[MultiClientTest]testMultiPUT, LFU");
					}
				}
				
			});
			
			threadPool[10] = new Thread(new Runnable() {

				@Override
				public void run() {
					String key = "key10";
					String value = "value10";
					
					try {
						server3.putKV(key, value);
					} catch (Exception e) {
						fail("[MultiClientTest]testMultiPUT, LFU");
					}
				}
				
			});
			
			threadPool[0].start();
			threadPool[1].start();
			threadPool[2].start();
			threadPool[3].start();
			threadPool[4].start();
			threadPool[5].start();
			threadPool[6].start();
			threadPool[7].start();
			threadPool[8].start();
			threadPool[9].start();
			threadPool[10].start();
			
			for (int i = 0; i < 11; i++) {
				threadPool[i].join();
			}
			
			
			String key = null;
			String value = null;
			for (int i = 0; i < 11; i++) {
				key = "key" + (new Integer(i)).toString();
				value = "value" + (new Integer(i)).toString();
				
				assertEquals(server3.getKV(key), value);
			}
			
		} catch (Exception e) {
			fail("[MultiClientTest]testMultiPUTLRU: Exception");
		} finally {
//			server3.close();
			server3.clearStorage();
		}
		
	}

}
