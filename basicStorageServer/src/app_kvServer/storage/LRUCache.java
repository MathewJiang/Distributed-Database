package app_kvServer.storage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import shared.messages.KVMessage.StatusType;

public class LRUCache {
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
	
	public static void clearCache(){
		if (hashmap != null)
			hashmap.clear();
	}
	
	public static boolean inCache(String key) {
		if(hashmap.containsKey(key)) {
			return true;
		} 
		return false;
	}
	
	public static String getKV(String key) throws Exception {
		String result;
		if(hashmap.containsKey(key)) {
			result = hashmap.get(key);
		} else {
			result = Disk.getKV(key);
	
			// result get successfully here
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
			hashmap.put(key,result);
		}
		
		queue.remove(key);
		queue.add(key);
		
		return result;
	}
	
	public static StatusType putKV(String key, String value) throws IOException{
		echo("DEBUG putKV key \"" + key+"\" value \"" + value + "\"");
		if(value == null || value.equals("")) {
			if(hashmap.containsKey(key)) {
				hashmap.remove(key);
				queue.remove(key);
				Disk.putKV(key, value);
				echo("DEBUG in hashmap putKV key " + key+" value " + value);
				return StatusType.DELETE_SUCCESS;
			} else {
				echo("DEBUG in disk putKV key " + key+" value " + value);
				return Disk.putKV(key, value);
			}
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
		
		if(hashmap.containsKey(key)) {
			if(hashmap.get(key).equals(value)) {
				return StatusType.PUT_SUCCESS; // if NOP needed, change this to new enum
			} else {
				hashmap.put(key,value);
				return StatusType.PUT_UPDATE;
			}
		} 
		hashmap.put(key,value);
		return StatusType.PUT_SUCCESS;
	}
	
	public static void flush_to_disk() throws IOException {
		Iterator<Map.Entry<String, String>> it = hashmap.entrySet().iterator();
	    while (it.hasNext()) {
	    	Map.Entry<String, String> pair = (Map.Entry<String, String>) it.next();
	    	Disk.floodKV(pair.getKey(), pair.getValue());
	        it.remove();
	    }
	}
}
