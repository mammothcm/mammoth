package org.apache.hadoop.mapred;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class CachePool {
	public static class CachePoolFullException extends IOException {
		CachePoolFullException(String s) {
			super(s);
		}
	}	
	public static class CacheUnit extends ByteArrayInputStream {		
		static int cap = 0;		
		private boolean isLast = false;
		public CacheUnit() {			
			super(new byte[cap]);			
			count = 0;
		}
		public int write(InputStream in) throws IOException {
			if (count >= buf.length) {
				return 0;
			}
			int res = in.read(buf, count, buf.length - count);
			if (res < 0) {
				return res;
			}
			count += res;
			return res;
		}
		public int write(byte b[], int off, int len){
			len = Math.min(len, buf.length - count);			
			System.arraycopy(b, off, buf, count, len);
			count += len;
			return len;
		}
		public void writeFile(OutputStream out) throws IOException {
			out.write(buf, mark, count);
		}
		public boolean write(int b){
			if (count >= buf.length) {
			    return false;
			}
			buf[count++] = (byte)b;
			return true;
	  }		
		public int getPos() {
			return pos;
		}
		public int getLen() {
			return count;
		}
		public void reinit(){
			count = 0;
			mark = 0;
			pos = 0;			
			isLast = false;
		}
		public boolean isLast() {
			return isLast;
		}
		public void setLast() {
			isLast = true;
		}
	}
	
	static private long upLine;
	private List<CacheUnit> pool = new LinkedList<CacheUnit>();
	private int busyNum = 0;
	static CachePool cp = null;
	static public void init(long up, int unitSize) {
		upLine = up;
		CacheUnit.cap = unitSize;
	}
	
	public static CachePool get() {
		if (cp == null) {
			cp = new CachePool();
		} 
		return cp;
	}
	public synchronized List<CacheUnit> getUnits(int num) throws CachePoolFullException {
		List<CacheUnit> res = new ArrayList<CacheUnit>(num);
		if (pool.size() >= num) {
			busyNum += num;
			res.addAll(pool.subList(0, num));
			pool.subList(0, num).clear();
			return res;			
		} else if ((busyNum + pool.size()) * CacheUnit.cap >= upLine){
			throw new CachePoolFullException("current cache size " + (busyNum * CacheUnit.cap) 
					+ " exceed upline " + upLine);
		} else {
			busyNum += num;
			for (int i = 0; i < num; i++) {
				res.add(new CacheUnit());
			}
			return res;
		}	
	}
	
	public synchronized CacheUnit getUnit() throws CachePoolFullException {
		if (!pool.isEmpty()) {
			busyNum++;
			return pool.remove(0);			
		} else if ((busyNum + pool.size()) * CacheUnit.cap >= upLine){
			throw new CachePoolFullException("current cache size " + (busyNum * CacheUnit.cap) 
					+ " exceed upline " + upLine);
		} else {
			busyNum++;
			return new CacheUnit();
		}	
	}
	public synchronized void returnUnit(CacheUnit cu) {
		busyNum--;
		cu.reinit();
		pool.add(cu);
	}
	public synchronized void returnUnit(List<CacheUnit> l) {
		busyNum -= l.size();
		for (CacheUnit cu : l) {
			cu.reinit();
			pool.add(cu);
		}		
	}	
}
