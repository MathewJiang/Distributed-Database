package app_kvServer.storage;

import java.io.IOException;

import app_kvServer.IKVServer.CacheStrategy;
import shared.messages.KVMessage.StatusType;

public class Storage {
	private static int mode = -1;
	private static OptimizedLRUCache optimizedLRUCache = new OptimizedLRUCache();
	private static LFUCache instanceLFUCache = new LFUCache();

	// mode 0 FIFO, 1 LRU, 2 LFU

	public static int getMode() {
		return mode;
	}

	public static void init(int cache_size) {
		Disk.init();
		// Disk.clearStorage();

		switch (mode) {
		case 0:
			FIFOCache.set_cache_size(cache_size);
			break;
		case 1:
			// LRUCache.set_cache_size(cache_size);
			optimizedLRUCache.setCacheSize(cache_size);
			break;
		case 2:
			instanceLFUCache.setCacheSize(cache_size);
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
			mode = -1;
			return false;
		}
		return true;
	}

	public static String getKV(String key) throws Exception {

		//Disk.echo("getKV: key: " + key);

		switch (mode) {
		case 0:
			return FIFOCache.getKV(key);
		case 1:
			// String result = LRUCache.getKV(key);
			return optimizedLRUCache.getKV(key);
		case 2:
			return instanceLFUCache.getKV(key);
		default:
			return Disk.getKV(key); // Update: deafult set to disk storage
		}

	}

	public static StatusType putKV(String key, String value) throws IOException {

		if (key == null || key.equals("")) {
			return StatusType.PUT_ERROR;
		}

		if (key.indexOf(' ') != -1) {
			return StatusType.PUT_ERROR;
		}

		if (key.length() > 20) {
			return StatusType.PUT_ERROR;
		}

		switch (mode) {
		case 0:
			return FIFOCache.putKV(key, value);
		case 1:
			return optimizedLRUCache.putKV(key, value);
			// return LRUCache.putKV(key, value);
		case 2:
			return instanceLFUCache.putKV(key, value);
		default:
			return Disk.putKV(key, value); // Update: default set to disk
											// storage
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
			optimizedLRUCache.clearCache();
			// LRUCache.clearCache();
			break;
		case 2:
			instanceLFUCache.clearCache();
			break;
		default:
			// do nothing
			break;
		}
	}

	public static boolean inCache(String key) {
		Disk.echo("LRU: inCache \"" + key + "\"");
		switch (mode) {
		case 0:
			return FIFOCache.inCache(key);
		case 1:
			return optimizedLRUCache.inCache(key);
			// return LRUCache.inCache(key);
		case 2:
			return instanceLFUCache.inCache(key);
		default:
			return false;
		}
	}

	public static boolean inStorage(String key) {
		Disk.echo("LRU: inStorage \"" + key + "\"");
		return Disk.inStorage(key);
	}

	public static void flush() throws IOException {
		switch (mode) {
		case 0:
			FIFOCache.flush_to_disk();
			break;
		case 1:
			optimizedLRUCache.flushToDisk();
			// LRUCache.flush_to_disk();
			break;
		case 2:
			instanceLFUCache.flushToDisk();
			break;
		default:
			break;
		}
	}
}
