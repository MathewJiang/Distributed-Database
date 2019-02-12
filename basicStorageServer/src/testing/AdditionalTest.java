package testing;

import org.junit.Test;

import junit.framework.TestCase;


public class AdditionalTest extends TestCase {
	
	// TODO add your test cases, at least 3
	static String path = "";
	
	@Test
	public void testStub() {
		
		path = System.getProperty("user.dir");

		//testFuncFifoCache();
		/*Disk.init();
		if(Disk.if_init()) {
			Disk.clearStorage();
		} else {
			Disk.echo("Disk init failed");
		}
		
		StorageTest.testFuncFifoCache();*/
//		StorageTest.testDiskPerf_config();
		//StorageTest.StoragePerfTestSweep();
//		StorageTest.ServerPerfTestSweep();
		
		StorageTest.testFuncLRUCache();
		StorageTest.testFuncLFUCache();
	}
	
}
