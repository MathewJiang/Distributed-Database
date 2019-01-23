package testing;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.junit.Test;

import app_kvServer.storage.Disk;
import app_kvServer.storage.FIFOCache;
import junit.framework.TestCase;


public class AdditionalTest extends TestCase {
	
	// TODO add your test cases, at least 3
	static String path = "";
	
	@Test
	public void testStub() {
		
		path = System.getProperty("user.dir");

		//testFuncFifoCache();
		Disk.init();
		if(Disk.if_init()) {
			Disk.clearStorage();
		} else {
			Disk.echo("Disk init failed");
		}
		
		StorageTest.testFuncFifoCache();
	}
	
}
