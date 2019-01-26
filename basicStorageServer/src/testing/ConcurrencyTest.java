package testing;


import org.junit.Before;
import org.junit.Test;
import junit.framework.TestCase;

import app_kvClient.IKVClient;
import app_kvServer.IKVServer;
import shared.ObjectFactory;
import app_kvServer.KVServer;

public class ConcurrencyTest extends TestCase {

	private IKVClient client1 = null;
	private IKVClient client2 = null;
	private IKVClient client3 = null;
	private IKVClient client4 = null;
	private IKVClient client5 = null;
	
	private IKVServer server1 = null;
	private IKVServer server2 = null;
	private IKVServer server3 = null;
	private IKVServer server4 = null;
	private IKVServer server5 = null;
	
	@Before
	protected void setUp() {
		try {
			server1 = ObjectFactory.createKVServerObject(0, 5, "FIFO");
			server2 = ObjectFactory.createKVServerObject(0, 5, "LRU");
			server3 = ObjectFactory.createKVServerObject(0, 5, "LFU");
			
			((KVServer)server1).start();
			((KVServer)server2).start();
			((KVServer)server3).start();
//			server4 = ObjectFactory.createKVServerObject(0, 5, "FIFO");
//			server5 = ObjectFactory.createKVServerObject(0, 5, "FIFO");
			
			//start multiple client
//			client1 = ObjectFactory.createKVClientObject();
//			client2 = ObjectFactory.createKVClientObject();
//			client3 = ObjectFactory.createKVClientObject();
//			client4 = ObjectFactory.createKVClientObject();
//			client5 = ObjectFactory.createKVClientObject();
//			
//			client1.newConnection("localhost", server1.getPort());
//			client2.newConnection("localhost", server2.getPort());
//			client3.newConnection("localhost", server3.getPort());
//			client4.newConnection("localhost", server4.getPort());
//			client5.newConnection("localhost", server5.getPort());
			
		} catch (Exception e){
			e.printStackTrace();
		}
	}

	@Test
	public void testMultiPUTFIFO() {
		assertNotNull(server1);
		
		server1.clearStorage();
		
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
				
				assertEquals(server1.getKV(key), value);
			}
			
		} catch (Exception e) {
			fail("[MultiClientTest]testMultiPUT: Exception");
		} finally {
			server1.close();
			server1.clearStorage();
		}
		
	}
	
	
	@Test
	public void testMultiPUTLRU() {
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
			server2.close();
			server2.clearStorage();
		}
		
	}
	
	@Test
	public void testMultiPUTLFU() {
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
			server3.close();
			server3.clearStorage();
		}
		
	}

}
