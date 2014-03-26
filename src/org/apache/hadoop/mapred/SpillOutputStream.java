package org.apache.hadoop.mapred;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapred.CachePool.CachePoolFullException;
import org.apache.hadoop.mapred.CachePool.CacheUnit;

public class SpillOutputStream extends OutputStream{

	private static final Log LOG = LogFactory.getLog(SpillOutputStream.class.getName());
	private List<CacheUnit> content;
	private final CachePool pool;
	private CacheUnit curCU = null;
	private long len = 0; 
	private boolean isWriteDone = false;	 
	
	int maxCus = 0;
	FSDataOutputStream output;
	private TaskAttemptID taskid;

	SpillOutputStream(CachePool p, long rlength, FSDataOutputStream out, 
			TaskAttemptID tid, int priority) throws IOException{
		maxCus = SpillScheduler.get().getMaxPerFileUpCus();
		content = new ArrayList<CacheUnit>(maxCus);
		pool = p;
		output = out;		
		taskid = tid;
		if (output == null) {
			throw new IOException("asynchronized write outputstream null!");
		}
		SpillScheduler.get().registerSpill(output, priority, this);
		content.addAll(pool.getUnits(maxCus));
		curCU = content.remove(0);
	}
	
	public synchronized void write(byte b[], int off, int length) throws CachePoolFullException {
		int m = length;
		while (m > 0) {
			int res = curCU.write(b, off + length - m, m);
			if (res < m) {
				SpillScheduler.get().addSpill(output, curCU);
				synchronized (content) {
					if(content.size() == 0) {
						try {
							content.wait();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					curCU = content.remove(0);	
				}
			}
			len += res;
			m -= res;
		}			
	}	
	
	public synchronized void write(int b) throws IOException {
		if (!curCU.write(b)) {
			SpillScheduler.get().addSpill(output, curCU);			
			synchronized (content) {
				if(content.size() == 0) {
					try {
						content.wait();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				curCU = content.remove(0);	
			}
			if (curCU.write(b)) {
				len++;
			} else {
				throw new IOException("CacheUnit cap = 0"); 
			}			
		} else {
			len++;
		}
  }

	public void close() throws IOException {       //for read
		if (isWriteDone) {			
			return;
		}
		isWriteDone = true;
		if (len != 0) {
			curCU.setLast();
			LOG.info(taskid + " close write!");
			SpillScheduler.get().addSpill(output, curCU);
			try {
				SpillScheduler.get().waitSpillFinished(output);
				output.close();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		SpillScheduler.get().unRegisterSpill(output);
		LOG.info(taskid + " close write done!");
		pool.returnUnit(content);
		content.clear();
	}
	
	public void returnUnit(CacheUnit cu) {
		synchronized(content) {
			cu.reinit();
			content.add(cu);
			content.notifyAll();
		}
	}
}
