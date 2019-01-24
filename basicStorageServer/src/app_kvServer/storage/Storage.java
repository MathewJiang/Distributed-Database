package app_kvServer.storage;

import java.io.IOException;

import app_kvServer.IKVServer.CacheStrategy;
import shared.messages.KVMessage;

public class Storage {
	static int mode;
	// mode 0 FIFO, 1 LRU, 2 LFU

	public static void init(int cache_size) {
		Disk.init();
		Disk.clearStorage();

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
		switch (mode) {
		case 0:
			return FIFOCache.getKV(key);
		case 1:
			return LRUCache.getKV(key);
		case 2:
			return LFUCache.getKV(key);
		default:
			return ""; // never reach here if mode is configured legally
		}
	}

	public static KVMessage.StatusType putKV(String key, String value) throws IOException {
		switch (mode) {
		case 0:
			return FIFOCache.putKV(key, value);
		case 1:
			return LRUCache.putKV(key, value);
		case 2:
			return LFUCache.putKV(key, value);
		default:
			return KVMessage.StatusType.PUT_ERROR; // never reach here if mode
													// is configured legally
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
			return;
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

}
