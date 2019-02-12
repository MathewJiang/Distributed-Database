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
import app_kvServer.storage.LFUCache;
import app_kvServer.storage.LRUCache;
import app_kvServer.storage.OptimizedLRUCache;
import app_kvServer.storage.Storage;
import app_kvServer.IKVServer.CacheStrategy;
import app_kvServer.KVServer;

public class StorageTest {

	@Test
	public void test() {
		// fail("Not yet implemented");
		testFuncLRUCache();
		testFuncLFUCache();
	}

	static String path = "";

	public static void testDiskPerf_config() {
		File test_log = new File(path + "perf.log");
		if (test_log.exists()) {
			test_log.delete();

		}
		try {
			test_log.createNewFile();
		} catch (IOException e) {
			
			e.printStackTrace();
		}

		/*
		 * int i = 100; while(i < 1000) { testDiskPerf(i); i+=100; }
		 */
	}

	public static long testDiskPerf(int num_files) {
		Disk.init(); // must init first
		Disk.clearStorage();

		final long startTime = System.currentTimeMillis();
		try {
			Disk.putKV("a", "1209");
		} catch (IOException e1) {
			
			e1.printStackTrace();
		}
		for (int i = 0; i < num_files; i++) {
			try {
				Disk.putKV(((Integer) i).toString(), ((Integer) i).toString());
			} catch (IOException e) {
				
				e.printStackTrace();
			}
		}

		try {
			String read = Disk.getKV("a");
			Disk.echo(read);
			read = Disk.getKV("8475");
			Disk.echo(read);
		} catch (Exception e) {
			
			e.printStackTrace();
		}
		final long endTime = System.currentTimeMillis();
		String perfTime = "num_files = " + num_files + " Total used " + (endTime - startTime) + "\n";
		append(path + "perf.log", perfTime);
		return (endTime - startTime);
	}

	public static void testFuncLRUCache() {
		Disk.init();
		Disk.clearStorage();
		OptimizedLRUCache instanceLRU = new OptimizedLRUCache();
		instanceLRU.setCacheSize(5);

		String[] data = new String[] { "a", "b", "c", "d", "e" };
		try {
			for (String kv : data) {
				instanceLRU.putKV(kv, kv);
			}

			// Ensure all data are populated into LRU cache.
			for (String kv : data) {
				assertTrue(instanceLRU.inCache(kv));
				assertTrue(kv.equals(instanceLRU.getKV(kv)));
			}

			// Eviction should happen at key "a".
			instanceLRU.putKV("new", "new");
			assertFalse(instanceLRU.inCache(data[0]));

			// Get keys from data backwards.
			for (int i = data.length - 1; i > 0; i--) {
				instanceLRU.getKV(data[i]);
			}
			instanceLRU.getKV("new");

			// At this point, eviction should happen at key "e"
			instanceLRU.putKV("new2", "new2");
			assertFalse(instanceLRU.inCache(data[data.length - 1]));
		} catch (Exception e) {
			// Unexpected error.
			e.printStackTrace();
			fail();
		} finally {
			Disk.clearStorage();
		}
	}

	public static void testFuncLFUCache() {
		Disk.init();
		Disk.clearStorage();
		LFUCache instanceLFU = new LFUCache();
		instanceLFU.setCacheSize(5);

		Integer[] data = new Integer[] { 4, 2, 5, 1, 3 };
		try {
			// Put data into LFU cache then refresh entry usages based on
			// integer inside data.
			for (Integer kv : data) {
				instanceLFU.putKV(kv.toString(), kv.toString());
				for (int i = 0; i < kv; i++) {
					instanceLFU.getKV(kv.toString());
				}
			}

			// Ensure all data are populated into LRU cache.
			// This process should increment all counters in LFU
			// and therefore has no effect on eviction order.
			for (Integer kv : data) {
				assertTrue(instanceLFU.inCache(kv.toString()));
				assertTrue(kv.toString().equals(instanceLFU.getKV(kv.toString())));
			}

			// Eviction, number "1" should be evicted.
			instanceLFU.putKV("new", "new");
			assertFalse(instanceLFU.inCache("1"));
			
			// Don't let this effect our test cases.
			for (int i = 0; i < 10; i++)
				instanceLFU.getKV("new");
			
			// Evict again, this time number "2" should drop.
			instanceLFU.putKV("new2", "new2");
			assertFalse(instanceLFU.inCache("2"));
			
			// "new2" was only "used" once during putKV. It 
			// should be evicted and "3" should stay in cache.
			instanceLFU.putKV("new3", "new3");
			assertFalse(instanceLFU.inCache("new2"));
			assertTrue(instanceLFU.inCache("3"));
			
		} catch (Exception e) {
			// Unexpected error.
			e.printStackTrace();
			fail();
		} finally {
			Disk.clearStorage();
		}
	}

	public static void testFuncFifoCache() {
		Disk.init();
		Disk.clearStorage();
		FIFOCache.set_cache_size(5);

		try {
			FIFOCache.putKV("iPhone", "XS");
			FIFOCache.putKV("Apple", "iPad");
			for (int i = 0; i < 10; i++) {
				FIFOCache.putKV(((Integer) i).toString(), ((Integer) (i + 1)).toString());
			}
			FIFOCache.putKV("ECE344", "DEADBEEF");
			FIFOCache.putKV("ECE454", "GOL");
		} catch (Exception e) {
			// to do
		}

		Disk.echo("disk has " + Disk.key_count() + " entires");

		String test;
		try {
			test = FIFOCache.getKV("Apple");
			if (test == "Apple") {
				Disk.echo("Key is not lost");
			}
		} catch (Exception e) {
			
			e.printStackTrace();
		}

		try {
			test = Disk.getKV("Apple");
			if (test.equals("iPad")) {
				Disk.echo("Pass, early keys should be in Disk");
			} else {
				Disk.echo("Fail, early keys should be in Disk and I got " + test);
			}
		} catch (Exception e) {
			
			e.printStackTrace();
		}

		try {
			test = Disk.getKV("ECE454");
			if (test.equals("GOL")) {
				Disk.echo("Fail, late keys should not be in Disk");
			} else {
				Disk.echo("Pass, late keys should not be in Disk");
			}
		} catch (Exception e) {
			Disk.echo("Expected");
		}
		Disk.echo("disk has " + Disk.key_count() + " entires");
		// fifoCache.flush_to_disk();
	}

	public static void append(String file_path, String content) {
		OutputStream os;
		try {
			os = new FileOutputStream(new File(file_path), true);
			os.write(content.getBytes(), 0, content.length());
			os.close();
		} catch (IOException e) {
			
			e.printStackTrace();
		}

	}

	public static void StoragePerfTestSweep() {
		for (int num_files = 1000; num_files < 20000; num_files += 1000) {
			double cache_ratio[] = { 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0 };
			double rw_ratio[] = { 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9 };
			int i_rw = 0;
			while (i_rw < 9) {
				int i_cache = 0;
				StoragePerfTest(num_files, 0, rw_ratio[i_rw], CacheStrategy.None, cache_ratio[i_cache]);
				while (i_cache < 10) {
					int cache_size = (int) ((num_files - (num_files * rw_ratio[i_rw])) * cache_ratio[i_cache]);
					ServerPerfTest(num_files, cache_size, rw_ratio[i_rw], CacheStrategy.FIFO, cache_ratio[i_cache]);
					ServerPerfTest(num_files, cache_size, rw_ratio[i_rw], CacheStrategy.LRU, cache_ratio[i_cache]);
					ServerPerfTest(num_files, cache_size, rw_ratio[i_rw], CacheStrategy.LFU, cache_ratio[i_cache]);
					i_cache++;
				}
				i_rw++;
			}
		}
	}

	public static void ServerPerfTestSweep() {
		for (int num_files = 1000; num_files < 20000; num_files += 1000) {
			double cache_ratio[] = { 0.2, 0.5, 0.8, 1.0 };
			double rw_ratio[] = { 0.2, 0.5, 0.8 };
			int i_rw = 0;
			while (i_rw < 3) {
				int i_cache = 0;
				ServerPerfTest(num_files, 0, rw_ratio[i_rw], CacheStrategy.None, cache_ratio[i_cache]);
				while (i_cache < 4) {
					int cache_size = (int) ((num_files - (num_files * rw_ratio[i_rw])) * cache_ratio[i_cache]);
					ServerPerfTest(num_files, cache_size, rw_ratio[i_rw], CacheStrategy.FIFO, cache_ratio[i_cache]);
					ServerPerfTest(num_files, cache_size, rw_ratio[i_rw], CacheStrategy.LRU, cache_ratio[i_cache]);
					ServerPerfTest(num_files, cache_size, rw_ratio[i_rw], CacheStrategy.LFU, cache_ratio[i_cache]);
					i_cache++;
				}
				i_rw++;
			}
		}
	}

	public static void StoragePerfTest(int num_files, int cache_size, double ratio, CacheStrategy strategy,
			double cache_ratio) {

		Storage.set_mode(strategy);
		Storage.init(cache_size);
		Storage.clearStorage();
		int num_reads = (int) ((int) num_files * ratio);
		int num_writes = num_files - num_reads;
		Disk.echo(
				"Setup test: # of reads: " + num_reads + " # of writes: " + num_writes + " cache_size: " + cache_size);
		String strategy_str = "";
		if (strategy == CacheStrategy.FIFO) {
			strategy_str = "FIFO";
		} else if (strategy == CacheStrategy.LRU) {
			strategy_str = "LRU";
		} else if (strategy == CacheStrategy.LFU) {
			strategy_str = "LFU";
		} else {
			strategy_str = "None";
		}
		append(path + "StoragePerfTest.log",
				"cache_ratio: " + cache_ratio + " Strategy " + strategy_str + " total: " + num_files + " # of reads: "
						+ num_reads + " # of writes: " + num_writes + " cache_size: " + cache_size + " ratio: "
						+ ratio);
		Disk.echo("Mode set " + strategy_str + " " + Storage.getMode());
		final long startTime = System.currentTimeMillis();
		for (int i = 0; i < num_writes; i++) {
			try {
				Storage.putKV(((Integer) i).toString(), ((Integer) i).toString());
			} catch (IOException e) {
				
				e.printStackTrace();
			}
		}
		while (num_reads != 0) {
			int randomNum = ThreadLocalRandom.current().nextInt(0, num_writes); // https://stackoverflow.com/questions/44379661/how-can-i-generate-a-random-number-within-a-certain-range-with-java
			try {
				Storage.getKV(((Integer) randomNum).toString());
			} catch (Exception e) {
				
				e.printStackTrace();
			}
			num_reads--;
		}
		final long endTime = System.currentTimeMillis();

		append(path + "StoragePerfTest.log", " time taken " + (endTime - startTime) + "\n");
	}

	public static void ServerPerfTest(int num_files, int cache_size, double ratio, CacheStrategy strategy,
			double cache_ratio) {
		
		String cStrategy = null;
		if (strategy.equals(CacheStrategy.FIFO)) {
			cStrategy = "FIFO";
		} else if (strategy.equals(CacheStrategy.LRU)) {
			cStrategy = "LRU";
		} else if (strategy.equals(CacheStrategy.LFU)) {
			cStrategy = "LFU";
		} else {
			cStrategy = "None";
		}
		KVServer server = new KVServer(5000, cache_size, cStrategy);
		Storage.set_mode(strategy);
		Storage.init(cache_size);
		Storage.clearStorage();
		int num_reads = (int) ((int) num_files * ratio);
		int num_writes = num_files - num_reads;
		Disk.echo(
				"Setup test: # of reads: " + num_reads + " # of writes: " + num_writes + " cache_size: " + cache_size);
		String strategy_str = "";
		if (strategy == CacheStrategy.FIFO) {
			strategy_str = "FIFO";
		} else if (strategy == CacheStrategy.LRU) {
			strategy_str = "LRU";
		} else if (strategy == CacheStrategy.LFU) {
			strategy_str = "LFU";
		} else {
			strategy_str = "None";
		}
		append(path + "ServerPerfTest.log",
				"cache_ratio: " + cache_ratio + " Strategy " + strategy_str + " total: " + num_files + " # of reads: "
						+ num_reads + " # of writes: " + num_writes + " cache_size: " + cache_size + " ratio: "
						+ ratio);
		Disk.echo("Mode set " + strategy_str + " " + Storage.getMode());
		final long startTime = System.currentTimeMillis();
		for (int i = 0; i < num_writes; i++) {
			try {
				server.putKV(((Integer) i).toString(), ((Integer) i).toString());
			} catch (IOException e) {
				
				e.printStackTrace();
			} catch (Exception e) {
				
				e.printStackTrace();
			}
		}
		while (num_reads != 0) {
			int randomNum = ThreadLocalRandom.current().nextInt(0, num_writes); // https://stackoverflow.com/questions/44379661/how-can-i-generate-a-random-number-within-a-certain-range-with-java
			try {
				server.getKV(((Integer) randomNum).toString());
			} catch (Exception e) {
				e.printStackTrace();
			}
			num_reads--;
		}
		final long endTime = System.currentTimeMillis();

		append(path + "ServerPerfTest.log", " time taken " + (endTime - startTime) + "\n");
	}

}