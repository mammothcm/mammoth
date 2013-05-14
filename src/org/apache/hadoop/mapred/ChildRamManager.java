package org.apache.hadoop.mapred;

import java.io.IOException;

import java.util.ArrayList;
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
	
	private static long maxSize = 0;
	private long size = 0;
	private long reduceSize = 0;	
	private int curMapsNum = 0;
	private long fullSize = 0;
	private static float maxInMemPer;
	private long mapRamHeap = 0;
	private int reduceIndex = 0;
	private ReentrantLock reserveLock = new ReentrantLock();
	private MapRamManager mapRM = null;
	private Map<TaskAttemptID, ReduceRamManager> reduceRMs 
		= new HashMap<TaskAttemptID, ReduceRamManager>();
	private List<RamManager> waitingRMs = new ArrayList<RamManager>();
	static ChildRamManager crm = null;			
	static public float reduceShuffleSortPer = 0f; 
	
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
        conf.getFloat("mapred.buf.total.inmem.percent", 0.70f);		
		reduceShuffleSortPer = conf.getFloat("reduce.shuffle.inMemSort.max.memPer", 0.4f);
	}
	public static long getMaxSize() {
		return maxSize;
	}
	//Singleton Pattern
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
	}
	
	
	
	public ReentrantLock getReserveLock() {
		return this.reserveLock;
	}
	
	public boolean tryReserve(TaskAttemptID tid, BufType type, long requestedSize) {
		try {
			reserveLock.lock();		
			if (tid == null || tid.isMap()) {
				return (size + requestedSize) > maxSize; 
			}else {
				return reduceRMs.get(tid).getReservedSize() + requestedSize > ReduceRamManager.maxSize;				
			}
		} finally {
			reserveLock.unlock();
		}				
	}
	
	public void reserve(TaskAttemptID tid, BufType type, long requestedSize) 
			throws InterruptedException {
		// Wait till the request can be fulfilled...
		try{
			reserveLock.lock();
			LOG.info("");
			if ((size + requestedSize) > maxSize && tid.isMap()) {			
				// Wait for memory to free up			
				if (!(waitingRMs.contains(mapRM))) {
					waitingRMs.add(0, mapRM);
					reduceIndex++;
				}								
				mapRM.await();				
			} else if (!tid.isMap()&&(reduceRMs.get(tid).getReservedSize() + requestedSize > ReduceRamManager.maxSize)){
				if (!waitingRMs.contains(reduceRMs.get(tid))) {
					waitingRMs.add(reduceRMs.get(tid));
				}
				reduceRMs.get(tid).await();
			}	else {
				if (!tid.isMap()) {
					reduceSize += requestedSize;	
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
		try {
			reserveLock.lock();
			fullSize += requestedSize;	
		} finally {
			reserveLock.unlock();
		}
	} 
	private void schedule() {
		if (waitingRMs.size() == 0) {		
			return;
		}		
		RamManager rm0 = waitingRMs.get(0);
		if (rm0 instanceof MapRamManager) {   
			//If map is blocked, current memory should be allocated to map first.
			//When memory is not enough for map, all should wait, 
			//even if current memory can be allocated to some reduce.		
			rm0.schedule();			
			while(rm0.getToReserveSize() != 0 && rm0.getToReserveSize() <= maxSize - size){
				size += rm0.getToReserveSize();				
				if (mapRamHeap == 0) {
					this.setPerReduceLimit();
				}
				rm0.awake();
				rm0.schedule();
			}
			if(rm0.getToReserveSize()==0) {
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
		List<RamManager> list = new ArrayList<RamManager>();
		list.addAll(waitingRMs);
		RamManager rm = list.get((reduceIndex) % list.size());
		while (list.size() > 0) {//poll until nothing can be scheduled。			
			rm.schedule();
			if (rm.getToReserveSize() <= 0) {
				waitingRMs.remove(rm);
				list.remove(rm);				
			} else if (rm.getToReserveSize() > maxSize - reduceSize - mapRamHeap * mapRM.getMapNum()) {
				list.remove(rm);				
			}	else {
				reduceSize += rm.getToReserveSize();
				size += rm.getToReserveSize();
				rm.awake();
			}
			if (list.size() == 0) {
				continue;
			}
			reduceIndex = (reduceIndex + 1) % list.size();
			rm = list.get((reduceIndex) % list.size());
		}
		if (waitingRMs.size() == 0) {
			reduceIndex = 0;
		} else {
			reduceIndex = (waitingRMs.indexOf(rm) + 1) % waitingRMs.size();
		}
	}
	public void unreserve(BufType type, long requestedSize) {
		try {
			reserveLock.lock();
			if (type.compareTo(BufType.copy)==0) {
				reduceSize -= requestedSize;
			} else if (mapRamHeap == 0) {
				this.setPerReduceLimit();
			}
			size -= requestedSize;			
			fullSize -= requestedSize;
			if (waitingRMs.size()==0) {			
				return;
			}			
			schedule();		
		} finally {
			reserveLock.unlock();
		}
		
	} 
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
		long max = (maxSize - mapMax) / reduceRMs.size();
		ReduceRamManager.setMaxMem(max);
	}
	
	public void setCurMapsNum(int num) {
		curMapsNum = num;
		this.setPerReduceLimit();
	}
	public static float getMaxInmemPer() {
		return maxInMemPer;
	}
}
