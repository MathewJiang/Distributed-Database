package app_kvServer;

import java.io.IOException;

import app_kvClient.Disk;
import shared.messages.KVMessage;

public class Storage {
	static int mode;
	// mode 0 FIFO, 1 LRU, 2 LFU
	
	public static void init() {
		Disk.init();
		Disk.clearStorage();
	}
	
	public static boolean set_mode(String mode_str) {
		if(mode_str.equals("FIFO")) {
			mode = 0;
		} else if(mode_str.equals("LRU")) {
			mode = 1;
		} else if(mode_str.equals("LFU")) {
			mode = 2;
		} else {
			return false;
		}
		return true;
	}
	
	public static String getKV(String key) throws Exception {
		switch(mode) {
			case 0:
				return fifoCache.getKV(key);
			case 1:
				return LRUCache.getKV(key);
			case 2:
				return LFUCache.getKV(key);
			default:
				return ""; // never reach here if mode is configured legally
		}
	}
	
	public static KVMessage.StatusType putKV(String key, String value) throws IOException{
		switch(mode) {
			case 0:
				return fifoCache.putKV(key,value);
			case 1:
				return LRUCache.putKV(key,value);
			case 2:
				return LFUCache.putKV(key,value);
			default:
				return KVMessage.StatusType.PUT_ERROR; // never reach here if mode is configured legally
		}
	}

	public static void clearStorage() {
		Disk.clearStorage();
	}
	
	public static void clearCache() {
		switch(mode) {
			case 0:
				fifoCache.clearCache();
			case 1:
				LRUCache.clearCache();
			case 2:
				LFUCache.clearCache();
			default:
				// do nothing
				return;
		}
	}
	
	public static boolean inCache(String key) {
		switch(mode) {
			case 0:
				fifoCache.inCache(key);
			case 1:
				LRUCache.inCache(key);
			case 2:
				LFUCache.inCache(key);
			default:
				return false;
		}
	}
	public static boolean inStorage(String key) {
		return Disk.inStorage(key);
	}

}
