package org.apache.hadoop.mapred;
import java.util.concurrent.locks.ReentrantLock;
public class RoundQueue<T> {
	class Segment {
		Segment(T t) {
			value = t;
			next = null;
		}
		T value;
		Segment next;
	}
	private ReentrantLock lock;
	private Segment last;
	private Segment head;
	private Segment ind;
	
	private int size;
	RoundQueue() {	
		last = null;
		head = null;

		size = 0;
		ind = null;
		lock = new ReentrantLock();
	}
	public void insert(T t) {
		try {
			lock.lock();
			if (size == 0) {
				head = new Segment(t);
				last = head;
				last.next = head;
				ind = head;		
				size++;				
			} else {
				last.next = new Segment(t);
				last = last.next;
				last.next = head;
				size++;
			}
		} finally {
			lock.unlock();
		}
	}
	private void reset() {
		last = null;
		head = null;

		ind = null;
		size = 0;
	}
	public T remove(T t) {
		try {
			lock.lock();
			if (size == 0 || head == null) {
				return null;
			}
			Segment q = head;
			Segment p = last;
			do {
				if (q.value == t || q.value.equals(t)) {
					size--;
					if (size == 0) {
						reset();
					} else {
						p.next = q.next;		
						if(head == q) {
							head = head.next;
						}
						if (last == q) {
							last = p;
						}
						if (ind == q) {
							ind = ind.next;
						}					
						
					}
					return q.value;					
				}
				p = q;
				q = q.next;
			}
			while (q != head);
			return null;
		} finally {
			lock.unlock();
		}		
	}	
	
	public T getNext() {
		try {
			lock.lock();
			if (ind == null || size == 0) {
				return null;
			}
			T r = ind.value;
			ind = ind.next;		
			return r;
		} finally {
			lock.unlock();
		}
	}
	public void list(String t) {
		if (size == 0 || head == null) {
			return;
		}
		t += (size + ": ");
		Segment s = head;
		do {
			t += (s.value + ", ");
			s = s.next;
		}while(s != head);
	}
	public int size() {
		return size;
	}	
}
