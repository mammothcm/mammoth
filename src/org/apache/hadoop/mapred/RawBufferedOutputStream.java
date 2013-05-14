package org.apache.hadoop.mapred;

import java.io.IOException;
import java.io.OutputStream;;

public class RawBufferedOutputStream  extends OutputStream{
	public static class BufferTooSmallException extends IOException {
    public BufferTooSmallException(String s) {
      super(s);
    }
  }
	protected byte[] buf;
	protected int count = 0;
	public RawBufferedOutputStream(byte[] b){
		buf = b;
	}
	public synchronized void write(byte b[], int off, int len) throws BufferTooSmallException {
		if (count + len >= buf.length) {
	    throw new BufferTooSmallException("buffer too small : " + buf.length);
		}
		System.arraycopy(b, off, buf, count, len);
		count += len;
	}	
	public synchronized void write(int b) throws BufferTooSmallException {
		if (count >= buf.length) {
		    throw new BufferTooSmallException("buffer too small : " + buf.length);
		}
		buf[count++] = (byte)b;
  }
}
