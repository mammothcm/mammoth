package org.apache.hadoop.mapred;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.CachePool.CacheUnit;
import org.apache.hadoop.util.StringUtils;

public class SpillScheduler extends Thread {	
	public static final int RECEIVE=0;
	public static final int SEND=1;
	public static final int SORT=2;
	
	private static final Log LOG = LogFactory.getLog(SpillScheduler.class.getName());
	class SpillFile{
		int priority;
		boolean finished;
		List<CacheUnit> cus;		
		SpillThread spillThread;
		SpillFile(SpillThread st, int prio) {
			priority = prio;
			spillThread = st;
			finished = false;
			cus = new LinkedList<CacheUnit>();
		}
		int size() {			
			return cus.size();			
		}
		void add(CacheUnit cu){				
			synchronized(cus) {
				while (cus.size() >= maxPerFileUpCus) {	
					try {										
						cus.wait(500);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}					
				}							
			}
			cus.add(cu);				
			spillThread.toSpillSize.getAndIncrement();
		}
		CacheUnit getNext(){				
			synchronized(cus) {
				if (cus.size() == 0) {
					return null;
				}			
				spillThread.toSpillSize.getAndDecrement();
				CacheUnit cu = cus.remove(0);				
				if (cus.size() + 1 >= maxPerFileUpCus) {
					cus.notify();
				}				
				return cu;
			}						
		}
	}
	
	class SpillThread extends Thread {
		Map<OutputStream, SpillFile> files = new ConcurrentHashMap<OutputStream, SpillFile>();		
		boolean stop = false;		
		int maxPriority = -1;
		boolean spilled = false;
		int round = 0;
		AtomicInteger toSpillSize = new AtomicInteger(0);
		OutputStream currentOs = null;
		private RoundQueue<OutputStream> spillIndeces =   //for schedule task to spill
				new RoundQueue<OutputStream>();
		public boolean containFile(OutputStream out) {			
			return files.containsKey(out);
		}
		public void addSpill(OutputStream out, CacheUnit cu){			
			if (files.get(out) == null) {
				LOG.info(" SpillScheduler don't contain this file! Please register first!");
				return;
			}
			int old = this.toSpillSize.get();
			files.get(out).add(cu);			
			if(old == 0) {
				synchronized (files) {
					files.notify();
				}
			}
		}
		private boolean hasSpillFinished(OutputStream out) {
			if (out == null || !files.containsKey(out) || files.get(out) == null) {			
				LOG.info("error hasSpillFinished");
				return true;
			} else {
				return files.get(out).finished;
			} 
		}
		
		public void registerSpill(OutputStream out, int priority) {
			if (files.containsKey(out)) {
				LOG.info(" out contained already");
				return;
			} else {				
				
				files.put(out, new SpillFile(this, priority));
				spillIndeces.insert(out);				
			}
		}
		public void unRegisterSpill(OutputStream out) {
				if (!files.containsKey(out)) {			
					return;
				}			
				spillIndeces.remove(out);		
				files.remove(out);		
		}
		private int getNumCus() {			
			int n = 0;
			for(SpillFile sf : files.values()) {
				n += sf.size();
			}
			return n;
		}
		
		private OutputStream getNextOs() {
			
			while (toSpillSize.get() > 0) {
				OutputStream out = spillIndeces.getNext();
				round++;
				if (round >= spillIndeces.size()) {
					if (!spilled) {
						maxPriority = -1;						
					}
					spilled = false;
					round = 0;
				}
				if (out == null) {
					return null;
				}
				if (files.get(out).size() > 0) {					
					return out;
				}
			}
			return null;
		}
		public void run() {
			while(!stop) {
				try {					
					synchronized (files) {				
						while (toSpillSize.intValue() == 0) {
							files.wait(500);					
						}
					}				
					currentOs = getNextOs();					
					if (currentOs == null) {
						String t = "";
						int i = 0;
						for (SpillFile sf : files.values()) {
							t += (", " + sf.size());
							i += sf.size();
						}
						t += ("toSpillSize: " + toSpillSize.intValue());
						toSpillSize.getAndSet(i);
						LOG.info(t);						
						continue;
					}					
					SpillFile sf = files.get(currentOs);		
					if (sf.priority < maxPriority) {
						continue;
					}	else {						
						maxPriority =sf.priority;
						spilled = true;
					}
					CacheUnit cu = sf.getNext();
					cu.writeFile(currentOs);					
					if (cu.isLast()) {						
						synchronized(currentOs) {							
							LOG.info(" file written finished");
							sf.finished = true;						
							currentOs.notify();
						}
					}					
					CachePool.get().returnUnit(cu);				
					currentOs = null;					
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					LOG.warn(" interupted " + e);
					e.printStackTrace();
				} catch (IOException e) {
					LOG.error(" write error! " + e);
					e.printStackTrace();
					// to be improved
				} catch (Throwable throwable) {
					String t = Thread.currentThread() + " files: " ;
					for (OutputStream out : files.keySet()) {
						t += (out + ", ");
					}
					LOG.info(t);
					t = Thread.currentThread() + " spillIndeces: " ;
					spillIndeces.list(t);
					LOG.info(t);
					LOG.fatal("Error running spill thread " + Thread.currentThread() +
						": " + StringUtils.stringifyException(throwable));
				}
			}
		}
	}
	private static SpillScheduler ss = null;												//singleton pattern
	private final int numMaxThreads;
	private final int maxPerFileUpCus;
	private boolean stop = false;
	Map<OutputStream, Integer> file2Threads = new ConcurrentHashMap<OutputStream, Integer>();
	private SpillThread[] spillThreads; 
	private ThreadGroup threadGroup = new ThreadGroup("spillThreads");	
	SpillScheduler() {		
		this(new JobConf());
	}	
	SpillScheduler(JobConf conf) {		
		super("spillSchedulerThread");
		numMaxThreads = conf.getInt("child.io.thread.num", 2);
		maxPerFileUpCus = conf.getInt("perfile.spillscheduler.cacheunit.max.num", 10);
		initialize();
	}
	
	public static SpillScheduler get() {
		if (ss == null) {
			ss = new SpillScheduler();			
		}
		return ss;
	}
	
	private void initialize() {		
		setDaemon(true);
		threadGroup.setDaemon(true);
		LOG.info(" numMaxThreads: " + numMaxThreads);
		spillThreads = new SpillThread[numMaxThreads];
		for (int i = 0; i < numMaxThreads; i++) {
			spillThreads[i] = new SpillThread();
			spillThreads[i].setDaemon(true);
			spillThreads[i].setName("spillThread" + i);
		}				
	}	
	public void run() {
		while (!stop) {
			for(SpillThread st : spillThreads) {
				if (Thread.State.TERMINATED.equals(st.getState()) || !st.isAlive()) {
					LOG.info(st + " down! ");					
					st.start();
				}
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	public  synchronized void registerSpill(OutputStream out, int priority) {
		int min = spillThreads[0].files.size();
		int ind = 0;
	//	test();
		for (int i = 1; i < spillThreads.length; i++) {
			int n = spillThreads[i].files.size();
			if (min > n) {
				min = n;
				ind =i;
			}
		}			
		spillThreads[ind].registerSpill(out, priority);
		this.file2Threads.put(out, new Integer(ind));
	}
	public synchronized void unRegisterSpill(OutputStream out) {
		int ind = this.file2Threads.get(out);
		if (ind < 0 || ind > spillThreads.length) {
			LOG.info(" SpillScheduler don't contain this file! Please register first!");
			return;
		}
		spillThreads[ind].unRegisterSpill(out);
		this.file2Threads.remove(out);
	}
	
	public void addSpill(OutputStream out, CacheUnit cu) {
		int ind = this.file2Threads.get(out);
		if (ind < 0 || ind > spillThreads.length) {
			LOG.info(" SpillScheduler don't contain this file! Please register first!");
			return;
		}		
		spillThreads[ind].addSpill(out, cu);		
	}
	private boolean hasSpillFinished(OutputStream out) {
		int ind = this.file2Threads.get(out);
		if (ind < 0 || ind > spillThreads.length) {
			LOG.info(" SpillScheduler don't contain this file! Please register first!");
			return true;
		}
		return spillThreads[ind].hasSpillFinished(out);		
	}
	public void waitSpillFinished(OutputStream out) throws InterruptedException {
		if (out == null) {
			return;
		}	
		synchronized (out) {
			while (!hasSpillFinished(out)) {		
				out.wait(500);
			}	
		}
	}
}
