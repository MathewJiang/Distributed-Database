package testing;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.junit.Test;

import app_kvClient.Disk;
import app_kvServer.fifoCache;

public class StorageTest {

	@Test
	public void test() {
		fail("Not yet implemented");
	}
	
	
	static String path = "";
	
	public void testDiskPerf_config() {
		File test_log = new File(path+"perf.log");
		if(test_log.exists()) {
			test_log.delete();
			
		}
		try {
			test_log.createNewFile();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		int i = 100;
		while(i < 1000) {
			testDiskPerf(i);
			i+=100;
		}
	}
	
	public long testDiskPerf(int num_files) {
    	Disk.init(); // must init first
    	Disk.clearStorage();
		
    	final long startTime = System.currentTimeMillis();
    	Disk.putKV("a", "1209");
    	for(int i = 0; i < num_files; i++) {
    		Disk.putKV(((Integer)i).toString(), ((Integer)i).toString());
    	}
    	
    	try {
			String read = Disk.getKV("a");
			Disk.echo(read);
			read = Disk.getKV("8475");
			Disk.echo(read);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	final long endTime = System.currentTimeMillis();
    	String perfTime = "num_files = " + num_files + " Total used " + (endTime - startTime) + "\n";
    	append(path+"perf.log", perfTime);
    	return (endTime - startTime);
	}
	
	public static void testFuncFifoCache() {
		Disk.init();
		Disk.clearStorage();
		fifoCache.set_cache_size(5);
		fifoCache.putKV("iPhone","XS");
		fifoCache.putKV("Apple","iPad");
		for(int i =0; i<10;i++) {
			fifoCache.putKV(((Integer)i).toString(), ((Integer)(i+1)).toString());
		}
		fifoCache.putKV("ECE344","DEADBEEF");
		fifoCache.putKV("ECE454","GOL");
		String test;
		try {
			test = fifoCache.getKV("Apple");
			if(test == "Apple") {
				Disk.echo("Key is not lost");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			test = Disk.getKV("Apple");
			if(test.equals("iPad")) {
				Disk.echo("Pass, early keys should be in Disk");
			} else {
				Disk.echo("Fail, early keys should be in Disk and I got " + test);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			test = Disk.getKV("ECE454");
			if(test.equals("GOL")) {
				Disk.echo("Fail, late keys should not be in Disk");
			} else {
				Disk.echo("Pass, late keys should not be in Disk");
			}
		} catch (Exception e) {
			Disk.echo("Expected");
		}
		Disk.echo("disk has "+Disk.key_count()+" entires");
		fifoCache.flush_to_disk();
	}
	
	public static void append(String file_path, String content) {
		OutputStream os;
		try {
			os = new FileOutputStream(new File(file_path), true);
			os.write(content.getBytes(), 0, content.length());
			os.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}


}
