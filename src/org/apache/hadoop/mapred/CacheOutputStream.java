package org.apache.hadoop.mapred;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.mapred.CachePool.CachePoolFullException;

public class CacheOutputStream extends OutputStream{
	protected CacheFile file;	
	public CacheOutputStream(CacheFile f){
		file = f;
	}
	public void write(byte b[], int off, int len)
			throws CachePoolFullException {
		try {
			file.write(b, off, len);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	
	public void write(int b)
			throws IOException{
		try {
			file.write(b);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
  }
	public void close() throws IOException {
		try {
			file.closeWrite();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
