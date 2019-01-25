package app_kvServer.storage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import app_kvServer.storage.OptimizedLRUCache.LinkedEntries.EntryNode;

import shared.messages.KVMessage.StatusType;

// Uses a double linked list and a hash map with list node references
// as values to implement a LRU cache that operates in constant times.
public class OptimizedLRUCache {

	static int cacheSize = -1;
	static Map<String, EntryNode> map;
	static LinkedEntries list;

	public static void setCacheSize(int size) {
		cacheSize = size;
		map = new HashMap<String, EntryNode>();
		list = new LinkedEntries();
	}

	public static void clearCache() {
		map.clear();
		list.clear();
	}

	public static boolean inCache(String key) {
		if (map.containsKey(key)) {
			return true;
		}
		return false;
	}

	public static String getKV(String key) throws Exception {
		EntryNode node;
		if (map.containsKey(key)) {
			node = map.get(key);
			// Relocate to the end of list.
			list.remove(node);
			list.addTail(node);
		} else {
			// Eviction.
			if (map.size() >= cacheSize) {
				EntryNode removeNode = list.getHead();
				list.remove(removeNode);
				map.remove(removeNode.key);

				// Write back to disk.
				Disk.putKV(removeNode.key, removeNode.value);
			}

			// Construct and cache new node.
			String result = Disk.getKV(key);
			node = list.new EntryNode(key, result);
			list.addTail(node);
			map.put(key, node);
		}
		return node.value;
	}

	public static StatusType putKV(String key, String value) throws IOException {
		if (value == null || value.equals("")) {
			if (map.containsKey(key)) {
				EntryNode toRemove = map.get(key);
				map.remove(toRemove.key);
				list.remove(toRemove);
				return StatusType.DELETE_SUCCESS;
			} else {
				return Disk.putKV(key, value);
			}
		}
		if (map.size() >= cacheSize) {
			EntryNode toRemove = list.getHead();
			map.remove(toRemove.key);
			list.remove(toRemove);

			// Need to write it to disk
			Disk.putKV(toRemove.key, toRemove.value);
		}
		EntryNode node = list.new EntryNode(key, value);
		list.addTail(node);
		if (map.containsKey(key)) {
			if (map.get(key).value.equals(value)) {
				return StatusType.PUT_SUCCESS; // if NOP needed, change this to
												// new enum.
			} else {
				map.put(node.key, node);
				return StatusType.PUT_UPDATE;
			}
		}
		map.put(node.key, node);
		return StatusType.PUT_SUCCESS;
	}

	public static void flushToDisk() throws IOException {
		Iterator<Map.Entry<String, EntryNode>> it = map.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, EntryNode> pair = (Map.Entry<String, EntryNode>) it
					.next();
			Disk.floodKV(pair.getKey(), pair.getValue().value);
			it.remove();
		}
		list.clear();
	}
	
	// TODO: Un-static this class after storage refactoring.
	public static class LinkedEntries {
		
		// Node of a double linked list.
		public class EntryNode {
			String key;
			String value;
			EntryNode prev;
			EntryNode next;

			public EntryNode() {
			}

			public EntryNode(String k, String v) {
				key = k;
				value = v;
			}
		}

		private EntryNode head;
		
		private EntryNode tail;

		public LinkedEntries() {
			head = new EntryNode();
			tail = new EntryNode();
			head.next = tail;
			tail.prev = head;
		}

		public EntryNode getHead() {
			return head.next;
		}

		public void addHead(EntryNode node) {
			node.prev = head;
			node.next = head.next;
			head.next.prev = node;
			head.next = node;
		}

		public EntryNode getTail() {
			return tail.prev;
		}

		public void addTail(EntryNode node) {
			node.prev = tail.prev;
			node.next = tail;
			tail.prev.next = node;
			tail.prev = node;
		}

		public void remove(EntryNode node) {
			node.prev.next = node.next;
			node.next.prev = node.prev;
		}

		public void clear() {
			head.next = tail;
			tail.prev = head;
		}
	}
}
