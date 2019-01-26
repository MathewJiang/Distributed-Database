package app_kvServer.storage;

import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;

import shared.messages.KVMessage.StatusType;

// The simplest Least Frequently Used Cache implemented by a counter
// heap and a hash map.
public class LFUCache {

	private class QueueEntry implements Comparable<QueueEntry> {
		BigInteger count;
		String key;
		String value;

		public QueueEntry(String k, String v) {
			key = k;
			value = v;
		}

		@Override
		public int compareTo(QueueEntry other) {
			return count.compareTo(other.count);
		}
	}

	static int cacheSize = -1;
	static PriorityQueue<QueueEntry> queue;
	static Map<String, QueueEntry> map;

	public static void setCacheSize(int size) {
		cacheSize = size;
		map = new HashMap<String, QueueEntry>();
		queue = new PriorityQueue<QueueEntry>();
	}

	public static void clearCache() {
		map.clear();
		queue.clear();
	}

	public static boolean inCache(String key) {
		if (map.containsKey(key)) {
			return true;
		}
		return false;
	}

	public String getKV(String key) throws Exception {
		QueueEntry entry;
		if (map.containsKey(key)) {
			entry = map.get(key);

			// Increment and re-insert into heap.
			queue.remove(entry);
			entry.count.add(BigInteger.ONE);
			queue.add(entry);
		} else {
			String result = Disk.getKV(key);

			// Eviction
			if (map.size() >= cacheSize) {
				QueueEntry removeEntry = queue.remove();
				map.remove(removeEntry.key);
				// Need to write it to disk
				Disk.putKV(removeEntry.key, removeEntry.value);
			}

			// Cache data retrieved from disk.
			entry = new QueueEntry(key, result);
			entry.count.add(BigInteger.ONE);
			queue.add(entry);
			map.put(entry.key, entry);
		}
		return entry.value;
	}

	public StatusType putKV(String key, String value) throws IOException {
		if (value == null || value.equals("")) {
			if (map.containsKey(key)) {
				QueueEntry removed = map.remove(key);
				queue.remove(removed);
				return StatusType.DELETE_SUCCESS;
			} else {
				return Disk.putKV(key, value);
			}
		}

		// Eviction
		if (map.size() >= cacheSize) {
			QueueEntry removeEntry = queue.remove();
			map.remove(removeEntry.key);
			// Need to write it to disk
			Disk.putKV(removeEntry.key, removeEntry.value);
		}

		QueueEntry entry = new QueueEntry(key, value);
		entry.count.add(BigInteger.ONE);
		queue.add(entry);

		if (map.containsKey(key)) {
			if (map.get(key).value.equals(value)) {
				return StatusType.PUT_SUCCESS; // if NOP needed, change this to
												// new enum
			} else {
				map.put(key, entry);
				return StatusType.PUT_UPDATE;
			}
		}
		map.put(key, entry);
		return StatusType.PUT_SUCCESS;
	}

	public static void flushToDisk() throws IOException {
		Iterator<Map.Entry<String, QueueEntry>> it = map.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, QueueEntry> pair = (Map.Entry<String, QueueEntry>) it
					.next();
			Disk.floodKV(pair.getKey(), pair.getValue().value);
			it.remove();
		}
		queue.clear();
	}
}
