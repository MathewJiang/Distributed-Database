package app_kvServer.storage;

public class LinkedEntries {
	
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
