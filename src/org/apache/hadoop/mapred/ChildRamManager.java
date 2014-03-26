package org.apache.hadoop.mapred;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

class ChildRamManager {	
	enum BufType{spill,merge,copy}
	private static final Log LOG = LogFactory.getLog(ChildRamManager.class.getName());
	
	private static final long MINREDUCEMAX = 100000000;
	private static final double REDUCECACHELIMIT = 0.95;
	
	private static long maxSize = 0;
	private long size = 0;
	private long reduceSize = 0;	
	private int curMapsNum = 0;
	private long fullSize = 0;
	private static float maxInMemPer;
	private long mapRamHeap = 0;
	private int reduceIndex = 0;
//	private int reduceSpillIndex = 0;
	private ReentrantLock reserveLock = new ReentrantLock();
	private MapRamManager mapRM = null;
	private Map<TaskAttemptID, ReduceRamManager> reduceRMs 
		= new HashMap<TaskAttemptID, ReduceRamManager>();
	private List<RamManager> waitingRMs = new ArrayList<RamManager>();
	static ChildRamManager crm = null;
	
//	private RamManager scheduled2SpillRM = null;
		
	static public float reduceShuffleSortPer = 0f; 
	//max percent of rest mem can be used for reduce sorting 
	//private long redHisFullSize = 0;
	//private int redHisClosedNum = 0;	
	//private long redHisUnreservedSize = 0;

	
	public ChildRamManager(int capacity) throws IOException {
		maxSize = capacity;		
	}
	
	public ChildRamManager() {
		// TODO Auto-generated constructor stub
	}
	
	public static void init(long max, Configuration conf) throws IOException {
		maxSize = max;			
		LOG.info("ChildRamManager: MemoryLimit=" + maxSize);
		maxInMemPer =
        conf.getFloat("mapred.buf.total.inmem.percent", 0.90f);		
		reduceShuffleSortPer = conf.getFloat("reduce.shuffle.inMemSort.max.memPer", 0.4f);
	}
	public static long getMaxSize() {
		return maxSize;
	}
	//单例模式
	public static ChildRamManager get() {  
		if(maxSize == 0) {
			LOG.info("maxSize : 0, init first!!");
			return null;
		}
		if(crm == null) {
			crm = new ChildRamManager();
		}
		return crm;
	}
	
	public void setMaxRamForMaps(long size) {		
		mapRamHeap = size;
		LOG.info("mapRamHeap : " + mapRamHeap);
	}
	
	
	
	public ReentrantLock getReserveLock() {
		return this.reserveLock;
	}
	
	public boolean tryReserve(TaskAttemptID tid, BufType type, long requestedSize) {
		long total = 0;
		for (ReduceRamManager rrm : reduceRMs.values()) {
			total += rrm.getReservedSize();
		}
		LOG.info("tryReserve requestedSize : " +  requestedSize + " mapHeap : " 
				+ mapRamHeap * mapRM.getMapNum() + " reduce max : " + 
				(maxSize - mapRamHeap * mapRM.getMapNum()) + " size : " + size +
				 " maxSize : " + maxSize + " map used : " + mapRM.getReservedSize() +
				 " reduce used : " + total); 
		try {
			reserveLock.lock();
			//boolean wait = false;
			if (tid == null || tid.isMap()) {
				return (size + requestedSize) > maxSize; 
			}else {
				return reduceRMs.get(tid).getReservedSize() + requestedSize > reduceRMs.get(tid).maxSize;				
			}
		} finally {
			reserveLock.unlock();
		}				
	}
	
	public void reserve(TaskAttemptID tid, BufType type, long requestedSize) 
			throws InterruptedException {
	//	LOG.info("reserve : " + type + " : " + requestedSize);
		// Wait till the request can be fulfilled...
		try{
			reserveLock.lock();
			if ((size + requestedSize) > maxSize && tid.isMap()) {			
				// Wait for memory to free up			
				if (!(waitingRMs.contains(mapRM))) {
					waitingRMs.add(0, mapRM);
					reduceIndex++;
				}								
				mapRM.await();				
			} else if (!tid.isMap()&&(reduceRMs.get(tid).getReservedSize() + requestedSize > reduceRMs.get(tid).maxSize)){
				if (!waitingRMs.contains(reduceRMs.get(tid))) {
					waitingRMs.add(reduceRMs.get(tid));
				}
				reduceRMs.get(tid).await();
			}	else {
				if (!tid.isMap()) {
					reduceSize += requestedSize;
			//		this.setPerReduceSortLimit();				
				} else if(mapRamHeap == 0) {
					this.setPerReduceLimit();
				}
				size += requestedSize;
			}
		} finally {
			reserveLock.unlock();
		}
	}

	public void closeInMemBuf(BufType type, long requestedSize) {
		// LOG.info("closeInMemBuf : " + type + " : " + requestedSize);
		try {
			reserveLock.lock();
			fullSize += requestedSize;
			if (type.compareTo(BufType.copy) == 0 ) {
				this.calculateReduce2SpillSize();
			} 
		} finally {
			reserveLock.unlock();
		}
	} 
	private void schedule() {
		if (waitingRMs.size() == 0) {		
			return;
		}		
		//LOG.info("schedule 333");
		RamManager rm0 = waitingRMs.get(0);
		if (rm0 instanceof MapRamManager) {   
			//如果有map阻塞，当前内存首先调度给map，若不够则等待更多内存释放
			rm0.schedule();			
			while(rm0.getToReserveSize() != 0 && rm0.getToReserveSize() <= maxSize - size){
				size += rm0.getToReserveSize();				
				if (mapRamHeap == 0) {
					this.setPerReduceLimit();
				}
				rm0.awake();
				rm0.schedule();
			}
			//LOG.info("schedule 444");
			if(rm0.getToReserveSize()==0) {
				//LOG.info("schedule +++");
				waitingRMs.remove(rm0);
				if (waitingRMs.size() == 0) {
					reduceIndex = 0;
					return;
				}			
				reduceIndex--;
			} else {
				return;
			}
		}		
		//LOG.info("schedule 555");
		List<RamManager> list = new ArrayList<RamManager>();
		list.addAll(waitingRMs);
		RamManager rm = list.get((reduceIndex) % list.size());
		while (list.size() > 0) {//轮询reduce，直至将当前内存分配完毕。			
			rm.schedule();
			//LOG.info("schedule 777");
			if (rm.getToReserveSize() <= 0) {
				waitingRMs.remove(rm);
				list.remove(rm);				
			} else if (rm.getToReserveSize() > maxSize - reduceSize - mapRamHeap * mapRM.getMapNum()) {
				list.remove(rm);				
			}	else {
				reduceSize += rm.getToReserveSize();
			//	this.setPerReduceSortLimit();
				size += rm.getToReserveSize();
				rm.awake();
			}
			if (list.size() == 0) {
				continue;
			}
			reduceIndex = (reduceIndex + 1) % list.size();
			rm = list.get((reduceIndex) % list.size());
			//LOG.info("schedule %%%");
		}
		//LOG.info("schedule 666");
		if (waitingRMs.size() == 0) {
			reduceIndex = 0;
		} else {
			reduceIndex = (waitingRMs.indexOf(rm) + 1) % waitingRMs.size();
		}
	}
	public void unreserve(BufType type, long requestedSize) {
		//	LOG.info("unReserve : " + type + " : " + requestedSize);
		try {
			reserveLock.lock();
			if (type.compareTo(BufType.copy)==0) {
				reduceSize -= requestedSize;
			//	this.setPerReduceSortLimit();
			} else if (mapRamHeap == 0) {
				this.setPerReduceLimit();
			}
			size -= requestedSize;
			
			fullSize -= requestedSize;
			if (waitingRMs.size()==0) {			
				return;
			}		
			//LOG.info("schedule 111");
			schedule();
		//	scheduleTask2Spill();    //reduce can have sufficient memory to sort in-mem.
		//	LOG.info("schedule 222");
		} finally {
			reserveLock.unlock();
		}
		
	} 
/*	public boolean exceedMemLimit(TaskAttemptID tid, BufType type) {
		String tmp = "";
		for (RamManager rm : reduceRMs.values()) {
			tmp += (" " + rm.getReservedSize()); 
		}
		LOG.info("maxsize : " + maxSize + " size : " + size + " fullsize : " + fullSize + " percenteUsed : " + 
				getPercentUsed() + " map used : " + mapRM.getReservedSize() + " reduce used : "
				+ tmp);
		
		if (scheduled2SpillRM == null) {
			return false;
		} else if (scheduled2SpillRM instanceof MapRamManager && tid == null ) {
			return true;
		} else if (scheduled2SpillRM instanceof ReduceRamManager && 
				((ReduceRamManager)scheduled2SpillRM).getTaskId().equals(tid) ) {
			return true;
		}		
		return false;
	}*/
	public float getPercentUsed() {
		return (float)(fullSize/maxSize);
	}
	public long getSize() {
		return size;
	}
	public long getFullSize() {
		return fullSize;
	}
	public void register(RamManager rm) {
		try {
			this.reserveLock.lock();
			if (rm instanceof MapRamManager) {
				mapRM = (MapRamManager)rm;
			} else if (rm instanceof ReduceRamManager){
				ReduceRamManager rrm = (ReduceRamManager)rm;
				reduceRMs.put(rrm.getTaskId(), rrm);
				this.setPerReduceLimit();
			}
		} finally {
			this.reserveLock.unlock();
		}
	}
	
	public void unregister(RamManager rm) {
		try {
			this.reserveLock.lock();
			if (rm instanceof MapRamManager) {
				mapRM = null;
			} else if (rm instanceof ReduceRamManager){			
				reduceRMs.remove(((ReduceRamManager)rm).getTaskId());
				this.setPerReduceLimit();
			}
		} finally {
			this.reserveLock.unlock();
		}
	}
	
	private void setPerReduceLimit() {
		if (reduceRMs.size() == 0) {
			return;
		}
		long mapMax = 0;
		if (mapRamHeap == 0) {
			mapMax = mapRM.getReservedSize();
		} else {
			mapMax = mapRM.getMapNum() * mapRamHeap;
		}		 
		long tmax = maxSize - mapMax;
		long total = 0;
		for (ReduceRamManager rrm : reduceRMs.values()) {
			if (rrm.isClosed()) {
				tmax -= rrm.getReservedSize();
			} else {
				total += rrm.getDesiredSize();
			}				
		}
		for (ReduceRamManager rrm : reduceRMs.values()) {
			if (!rrm.isClosed()) {
				long ds = rrm.getDesiredSize();
				long max = (long)((double)ds / (double)(total) * tmax);
				if (max < MINREDUCEMAX) {
					max = MINREDUCEMAX;					
				}				
				if (max > tmax) { 
					max = tmax;
				}
				rrm.setMaxMem(max);
				tmax -= max;
				total -= ds;
			}				
		}
	}
	
	
	public void setCurMapsNum(int num) {
		curMapsNum = num;
		this.setPerReduceLimit();
	}
	public static float getMaxInmemPer() {
		return maxInMemPer;
	}
	
	private long calSpillSize(List<Long> totals, long sum) {
		//sum: total to-spill size, totals: each reduce's total shuffle size
		//return: the max to-spill size for one reduce		
		Collections.sort(totals);
		int size = totals.size();
		long res = sum / size;
		for (Long i : totals) {
			long t = i.longValue();
			if (t < res) {
				sum -= t;
				size--;
				if (size == 0) {
					res = t;
					break;
				}
				res = sum / size;
			} else {
				break;
			}			
		}
		return res;
	}
	
	public void calculateReduce2SpillSize() {		
		if (this.reduceRMs.isEmpty()) {
			return;
		}				
		long total = 0;
		long tmax = maxSize;
		List<Long>  totals = new ArrayList<Long>(reduceRMs.size()); 
		for (ReduceRamManager rrm : this.reduceRMs.values()) {			
			if(!rrm.isClosed()) {
				long tss = rrm.getTotalShuffleSize();
				total += tss;
				totals.add(tss);
			} else {
				tmax -= rrm.getReservedSize();
			}
		}
		
		if (totals.size() == 0) {
			return;
		}
		
		if (total <= tmax) {
			for (ReduceRamManager rrm : this.reduceRMs.values()) {
				if(!rrm.isClosed()) {
					rrm.set2SpillSize(0);
				}
			}		
		} else {
			if (tmax < 0) {
				tmax = 0;
			}
			tmax *= REDUCECACHELIMIT;
			long tSpill = calSpillSize(totals, total - tmax);
			for (ReduceRamManager rrm : this.reduceRMs.values()) {
				if(!rrm.isClosed()) {
					rrm.set2SpillSize(tSpill);
				}
			}	
		}		
	}
	
	
}
