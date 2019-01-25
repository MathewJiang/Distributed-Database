package app_kvServer.storage;

public class LinkedEntries {
	
	// Node of a double linked list.
	public class EntryNode {
		String key;
		String value;
		EntryNode pre;
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
		tail.pre = head;
	}

	public EntryNode getHead() {
		return head.next;
	}

	public void addHead(EntryNode node) {
		node.pre = head;
		node.next = head.next;
		head.next.pre = node;
		head.next = node;
	}

	public EntryNode getTail() {
		return tail.pre;
	}

	public void addTail(EntryNode node) {
		node.pre = tail.pre;
		node.next = tail;
		tail.pre.next = node;
		tail.pre = node;
	}

	public void remove(EntryNode node) {
		node.pre.next = node.next;
		node.next.pre = node.pre;
	}

	public void clear() {
		head.next = tail;
		tail.pre = head;
	}
}
