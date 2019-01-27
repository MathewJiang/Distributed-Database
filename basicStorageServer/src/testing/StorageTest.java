package testing;

import static org.junit.Assert.*;

import java.util.concurrent.ThreadLocalRandom;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.junit.Test;

import app_kvServer.storage.Disk;
import app_kvServer.storage.FIFOCache;
import app_kvServer.storage.Storage;
import app_kvServer.IKVServer.CacheStrategy;
public class StorageTest {

	@Test
	public void test() {
		fail("Not yet implemented");
	}
	
	
	static String path = "";
	
	public static void testDiskPerf_config() {
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
		
		/*int i = 100;
		while(i < 1000) {
			testDiskPerf(i);
			i+=100;
		}*/
	}
	
	public static long testDiskPerf(int num_files) {
    	Disk.init(); // must init first
    	Disk.clearStorage();
		
    	final long startTime = System.currentTimeMillis();
    	try {
			Disk.putKV("a", "1209");
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    	for(int i = 0; i < num_files; i++) {
    		try {
				Disk.putKV(((Integer)i).toString(), ((Integer)i).toString());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
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
		FIFOCache.set_cache_size(5);
		
		try {
			FIFOCache.putKV("iPhone","XS");
			FIFOCache.putKV("Apple","iPad");
			for(int i =0; i<10;i++) {
				FIFOCache.putKV(((Integer)i).toString(), ((Integer)(i+1)).toString());
			}
			FIFOCache.putKV("ECE344","DEADBEEF");
			FIFOCache.putKV("ECE454","GOL");
		} catch (Exception e) {
			// to do
		}
		
		Disk.echo("disk has "+Disk.key_count()+" entires");
		
		String test;
		try {
			test = FIFOCache.getKV("Apple");
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
		//fifoCache.flush_to_disk();
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

	public static void StoragePerfTestSweep() {
		for(int num_files = 100; num_files < 10000; num_files+=100) {
			double cache_ratio[] = {0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0};
			double rw_ratio[] = {0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9};
			int i_rw = 0;
			while(i_rw < 9) {	
				int i_cache = 0;
				StoragePerfTest(num_files, 0, rw_ratio[i_rw], CacheStrategy.None, cache_ratio[i_cache]);
				while(i_cache < 10) {
					int cache_size = (int) ((num_files - (num_files * rw_ratio[i_rw])) * cache_ratio[i_cache]);
					StoragePerfTest(num_files, cache_size, rw_ratio[i_rw], CacheStrategy.FIFO, cache_ratio[i_cache]);
					StoragePerfTest(num_files, cache_size, rw_ratio[i_rw], CacheStrategy.LRU, cache_ratio[i_cache]);
					StoragePerfTest(num_files, cache_size, rw_ratio[i_rw], CacheStrategy.LFU, cache_ratio[i_cache]);
					i_cache++;
				}
				i_rw++;
			}
		}
	}
	

	public static void StoragePerfTest(int num_files, int cache_size, double ratio, CacheStrategy strategy, double cache_ratio) {
		
		Storage.set_mode(strategy);
		Storage.init(cache_size);
		Storage.clearStorage();
		int num_reads = (int) ((int)num_files * ratio);
		int num_writes = num_files - num_reads;
		Disk.echo("Setup test: # of reads: " + num_reads + " # of writes: " + num_writes + " cache_size: " + cache_size);
		String strategy_str = "";
		if(strategy == CacheStrategy.FIFO) {
			strategy_str = "FIFO";
		} else if(strategy == CacheStrategy.LRU) {
			strategy_str = "LRU";
		} else if(strategy == CacheStrategy.LFU) {
			strategy_str = "LFU";
		} else {
			strategy_str = "None";
		}
		append(path+"StoragePerfTest.log","cache_ratio: " + cache_ratio  + " Strategy " + strategy_str + " total: " + num_files + " # of reads: " + num_reads + " # of writes: " + num_writes + " cache_size: " + cache_size + " ratio: "+ratio);
		Disk.echo("Mode set " + strategy_str + " "+Storage.getMode());
		final long startTime = System.currentTimeMillis();
		for(int i = 0; i < num_writes; i++) {
    		try {
				Storage.putKV(((Integer)i).toString(), ((Integer)i).toString());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
		while(num_reads != 0) {
			int randomNum = ThreadLocalRandom.current().nextInt(0, num_writes); // https://stackoverflow.com/questions/44379661/how-can-i-generate-a-random-number-within-a-certain-range-with-java
			try {
				Storage.getKV(((Integer)randomNum).toString());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			num_reads --;
		}
		final long endTime = System.currentTimeMillis();
		
		append(path+"StoragePerfTest.log"," time taken " + (endTime - startTime) + "\n");
	}

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}