package app_kvServer.storage;

import java.io.IOException;

import app_kvServer.IKVServer.CacheStrategy;
import shared.messages.KVMessage.StatusType;

public class Storage {
	private static int mode = -1;
	// mode 0 FIFO, 1 LRU, 2 LFU

	public static int getMode() {
		return mode;
	}
	
	public static void init(int cache_size) {
		Disk.init();
//		Disk.clearStorage();

		switch (mode) {
			case 0:
				FIFOCache.set_cache_size(cache_size);
				break;
			case 1:
				LRUCache.set_cache_size(cache_size);
				break;
			case 2:
				LFUCache.set_cache_size(cache_size);
				break;
			default:
				return;
		}

	}

	public static boolean set_mode(CacheStrategy strategy) {
		switch (strategy) {
			case FIFO:
				mode = 0;
				break;
			case LRU:
				mode = 1;
				break;
			case LFU:
				mode = 2;
				break;
			default:
				return false;
		}
		return true;
	}

	public static String getKV(String key) throws Exception {
		
		Disk.echo("getKV: key: " + key);
		
		switch (mode) {
			case 0:
				return FIFOCache.getKV(key);
			case 1:
				String result = LRUCache.getKV(key);
				Disk.echo("gotKV: key: " + result);
				return result;
			case 2:
				return LFUCache.getKV(key);
			default:
				return Disk.getKV(key);		 	// Update: deafult set to disk storage
		}
		
		
	}

	public static StatusType putKV(String key, String value) throws IOException {
		Disk.echo("DEBUG storage putKV key \"" + key+"\" value \"" + value + "\"");
		if (key == null || key.equals("")) {
			return StatusType.PUT_ERROR;
		}
		
		if (key.indexOf(' ') != -1) {
			return StatusType.PUT_ERROR;
		}
		
		if (key.length() >20) {
			return StatusType.PUT_ERROR;
		}
		
		Disk.echo(key);
			
		switch (mode) {
			case 0:
				return FIFOCache.putKV(key, value);
			case 1:
				return LRUCache.putKV(key, value);
			case 2:
				return LFUCache.putKV(key, value);
			default:
				return Disk.putKV(key, value); 	// Update: default set to disk storage
		}
	}

	public static void clearStorage() {
		Disk.clearStorage();
	}

	public static void clearCache() {
		switch (mode) {
			case 0:
				FIFOCache.clearCache();
				break;
			case 1:
				LRUCache.clearCache();
				break;
			case 2:
				LFUCache.clearCache();
				break;
			default:
				// do nothing
				break;
		}
	}

	public static boolean inCache(String key) {
		switch (mode) {
			case 0:
				return FIFOCache.inCache(key);
			case 1:
				return LRUCache.inCache(key);
			case 2:
				return LFUCache.inCache(key);
			default:
				return false;
		}
	}

	public static boolean inStorage(String key) {
		return Disk.inStorage(key);
	}
	
	
	public static void flush() throws IOException {
		switch (mode) {
		case 0:
			FIFOCache.flush_to_disk();
			break;
		case 1:
			LRUCache.flush_to_disk();
			break;
		case 2:
			LFUCache.flush_to_disk();
			break;
		default:
			break;
	}
	}

}
