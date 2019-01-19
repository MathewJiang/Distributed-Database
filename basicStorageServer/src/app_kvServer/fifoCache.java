package app_kvServer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import app_kvClient.Disk;

public class fifoCache {
	static int cache_size = -1;
	static Map<String, String> hashmap;
	static Queue<String> queue;
	public static void echo(String line) {
		System.out.println(line);
	}
	
	public static void set_cache_size(int size) {
		cache_size = size;	
		echo("cache set as: " + cache_size);
		hashmap = new HashMap<String, String>();
		queue = new LinkedList<>();
	}
	
	public static String getKV(String key) throws Exception {
		String result;
		if(hashmap.containsKey(key)) {
			result = hashmap.get(key);
		} else {
			result = Disk.getKV(key);
		}
		return result;
	}
	
	public static void putKV(String key, String value){
		if(value.equals("null")) {
			hashmap.remove(key);
			queue.remove(key);
		}
		if(hashmap.size()>=cache_size) {
			// well, should be only ==
			// we need to evict one from the key list
			String removing_key = queue.remove();
			// Need to write it to disk
			Disk.putKV(removing_key, hashmap.get(removing_key));
			// remove it from cache
			hashmap.remove(removing_key);
		}
		queue.add(key);
		hashmap.put(key,value);
		return;
	}
	
	public static void flush_to_disk() {
		Iterator<Map.Entry<String, String>> it = hashmap.entrySet().iterator();
	    while (it.hasNext()) {
	    	Map.Entry<String, String> pair = (Map.Entry<String, String>) it.next();
	    	Disk.putKV(pair.getKey(), pair.getValue());
	        it.remove();
	    }
	}
}
