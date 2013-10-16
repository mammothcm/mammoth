package org.apache.hadoop.mapred;

import static org.apache.hadoop.mapred.Task.Counter.COMBINE_INPUT_RECORDS;
import static org.apache.hadoop.mapred.Task.Counter.COMBINE_OUTPUT_RECORDS;
import static org.apache.hadoop.mapred.Task.Counter.MAP_OUTPUT_BYTES;
import static org.apache.hadoop.mapred.Task.Counter.MAP_OUTPUT_MATERIALIZED_BYTES;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.CachePool.CacheUnit;
import org.apache.hadoop.mapred.IFile.Reader;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.mapred.Merger.Segment;
import org.apache.hadoop.mapred.Task.CombineOutputCollector;
import org.apache.hadoop.mapred.Task.CombinerRunner;
import org.apache.hadoop.mapred.Task.Counter;
import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;


public class DefaultJvmMemoryManager{	
	class DefaultMapRamManager implements MapRamManager{
		class MapReserveElement {
			MapReserveElement(TaskAttemptID tid) {
				taskId = tid;
				waitCond = crm.getReserveLock().newCondition();
				reserveSize = 0;
				waitTime = 0;
				reservedSize = 0;
				maxSize = 0;
			}
			long reservedSize;
			TaskAttemptID taskId;
			Condition waitCond;
			long reserveSize;
			long maxSize;
			int waitTime;			
		}
		
		private static final float MAX_STALLED_SPILL_THREADS_FRACTION = 0.75f;
		private ChildRamManager crm;		
		
		private Map<TaskAttemptID, MapReserveElement> mapREs = 
				new ConcurrentHashMap<TaskAttemptID, MapReserveElement>();
		private List<MapReserveElement> waitMREs = 
				new ArrayList<MapReserveElement>();  // waiting MRE Queue
		private TaskAttemptID curReserveID;
		private TaskAttemptID scheduledTask = null;
		private int maxWaitTime;
		private long size = 0;
		private long mapMaxHeapSize = 0;
		private final int maxInMemSpills;
		private int numRequiredMap = 0;
		private Object dataAvailable = new Object();
		private int numPendingRequests = 0;
		private int numClosed = 0;				
		private boolean isSpilling = false; 
		private long fullSize = 0;
		
		private boolean isScheduled2Spill = false;
		
		public DefaultMapRamManager(Configuration conf, ChildRamManager centralManager) 
		 throws IOException {		
			this.maxInMemSpills = conf.getInt("mapred.inmem.spill.num.threshold", 1000);
			maxWaitTime = conf.getInt("map.reserve.max.time", 3);
			crm = centralManager;
			crm.register(this);
		}
		
		public  void registerMap(TaskAttemptID tid) {			
			synchronized (dataAvailable) {
				mapREs.put(tid, new MapReserveElement(tid));
				this.numRequiredMap++;
				crm.setCurMapsNum(mapREs.size());
			}
		}
		
		public void unregisterMap(TaskAttemptID tid) {			
			synchronized (dataAvailable) {
				this.numRequiredMap--;				
				MapReserveElement mre = mapREs.remove(tid);
				if (mre.maxSize > mapMaxHeapSize) {
					mapMaxHeapSize = mre.maxSize; 
					crm.setMaxRamForMaps(mapMaxHeapSize);
				}
				crm.setCurMapsNum(mapREs.size());
				dataAvailable.notify();
			}
		}
		public void await() throws InterruptedException{
			try {
				crm.getReserveLock().lock();
				mapREs.get(curReserveID).waitCond.await();
			} finally {
				crm.getReserveLock().unlock();
			}
		}
		public void awake() {						
			waitMREs.remove(mapREs.get(scheduledTask));
			mapREs.get(scheduledTask).waitTime = 0;
			mapREs.get(scheduledTask).reserveSize = 0;
			for (MapReserveElement mre : waitMREs) {
				mre.waitTime++;
			}
			mapREs.get(scheduledTask).waitCond.signal();			
		}
		//true : should wait
		public boolean tryReserve(int requestedSize) { 
			return crm.tryReserve(null, ChildRamManager.BufType.spill, requestedSize);
		}
		public void reserve(TaskAttemptID tid, long requestedSize) 
			throws InterruptedException {	
			try {
				crm.getReserveLock().lock();
				boolean isWait = crm.tryReserve(tid, ChildRamManager.BufType.spill, requestedSize);
				MapReserveElement mre = mapREs.get(tid);
				if(isWait) {
					curReserveID = tid;				
					mre.reserveSize = requestedSize;				
					mre.waitTime = 0;
					boolean added = false;
					for (int i = 0; i < waitMREs.size(); i++) {
						if (mre.reserveSize < waitMREs.get(i).reserveSize) {
							waitMREs.add(i, mre);
							added = true;
							break;
						}
					}
					if (!added) {
						waitMREs.add(mre);
					}
					synchronized (dataAvailable) {
						++numPendingRequests;
						dataAvailable.notify();
					}
				}
				crm.reserve(tid, ChildRamManager.BufType.spill, requestedSize);
				if(isWait) {			
					synchronized (dataAvailable) {
						--numPendingRequests;					
					}
				}			   			
				size += requestedSize;
				mre.reservedSize += requestedSize;
				if (mre.maxSize < mre.reservedSize) {
					mre.maxSize = mre.reservedSize;
				}
			} finally {
				crm.getReserveLock().unlock();
			}
		}
		 
		public void unreserve(TaskAttemptID tid,int num, long requestedSize) {
			crm.unreserve(ChildRamManager.BufType.spill, requestedSize);
			synchronized (dataAvailable) {
				numClosed -= num;
				size -= requestedSize;
				fullSize -= requestedSize;
			}			
			mapREs.get(tid).reservedSize -= requestedSize;
		}
	 
		
		public void closeInMemorySpill(int requestedSize) {
			try {
				crm.getReserveLock().lock();				
				++numClosed;
				fullSize += requestedSize;
				crm.closeInMemBuf(ChildRamManager.BufType.spill, requestedSize);
			} finally {
				crm.getReserveLock().unlock();
			}
		}
	 	 
		//get to spill me nums
		private int getSpillMapNum() {
			int num = 0;
			for(MapSpiller ms : mapSpillers.values()) {
				if (ms.getState().compareTo(MapState.spill) == 0) {
					num += ms.getReadySpillNum();
				}
			}
			return num;
		}
		
		public void waitForDataToSpill() throws InterruptedException {		  
		  synchronized (dataAvailable) {
		  	// Start in-memory merge if manager has been closed or...
		  isSpilling = false;
		  	while( (
		  			// In-memory threshold exceeded and at least two segments
		  			// have been fetched
		  			(!isScheduled2Spill)
		  			&&
		  			// More than "mapred.inmem.merge.threshold" map outputs
		  			// have been fetched into memory
		  			(maxInMemSpills <= 0 || numClosed < maxInMemSpills)
		  			&& 
		  			// More than MAX... threads are blocked on the RamManager
		  			// or the blocked threads are the last map outputs to be
		  			// fetched. If numRequiredMapOutputs is zero, either
		  			// setNumCopiedMapOutputs has not been called (no map ouputs
		  			// have been fetched, so there is nothing to merge) or the
		  			// last map outputs being transferred without
		  			// contention, so a merge would be premature.
		  			((numRequiredMap < numJvmSlots || numPendingRequests < 
		  					numJvmSlots*MAX_STALLED_SPILL_THREADS_FRACTION) && 
		  					(0 >= numRequiredMap ||
		  					numPendingRequests < numRequiredMap)) ) || getSpillMapNum()==0){
		  		dataAvailable.wait();   
		  	}
		  	isScheduled2Spill = false;
		  	isSpilling = true;
		  }	  		  	  
		}

		public void schedule() {
			if (waitMREs.size()==0) {
				scheduledTask = null;
				return;
			}
			int maxWait = 0;
			int maxInd = 0;
			for (int i = 0; i < waitMREs.size(); i++) {
				MapReserveElement mre = waitMREs.get(i);
				if(mre.waitTime > maxWait) {
					maxWait = mre.waitTime;
					maxInd = i;
				}
			}
			if (maxWait > maxWaitTime) {
				scheduledTask = waitMREs.get(maxInd).taskId;
			} else {
				scheduledTask = waitMREs.get(0).taskId;
			}
		}

		@Override
		public long getToReserveSize() {
			// TODO Auto-generated method stub
			if (scheduledTask == null || waitMREs.size() == 0) {
				return 0;
			} else {
				return mapREs.get(scheduledTask).reserveSize; 
			}
		}

		@Override
		public long getReservedSize() {
			// TODO Auto-generated method stub
			return size;
		}

		@Override
		public int getMapNum() {
			// TODO Auto-generated method stub
			return this.numRequiredMap;
		}

		@Override
		public long getMapHeap() {
			// TODO Auto-generated method stub
			return mapMaxHeapSize;
		}

		@Override
		public void spill2Disk() {
			// TODO Auto-generated method stub
			synchronized(dataAvailable) {
				if (isScheduled2Spill || isSpilling) {
					return;
				}
				isScheduled2Spill = true;
				dataAvailable.notify();
			}
		}

		@Override
		public boolean isSpilling() {
			// TODO Auto-generated method stub
			return isSpilling;
		}
		
		public long getFullSize() {
			return fullSize;
		}
	}
	enum MapState{spill, merge}
 	class MapSpiller<K,V> {
		MapState state = MapState.spill;
		private ArrayList<SpillRecord> indexCacheList;
		private TaskAttemptID taskId;
		private int numSpills = 0;
		private int numSpilled = 0;
		protected final Counters.Counter spilledRecordsCounter;
	  private final CombinerRunner combinerRunner;
	  private final CombineOutputCollector combineCollector;
	  private final Counters.Counter combineOutputCounter;
	  private final Counters.Counter fileOutputByteCounter;
	  private final Counters.Counter mapOutputByteCounter;
	  
	  private RawKeyValueIterator rKVIter = null;  
	  private boolean isKilled = false;
	  
	  private final JobConf conf;
	  private final TaskReporter reporter;
	  private MapOutputFile mapOutputFile = new MapOutputFile();
	  
	  private int totalIndexCacheMemory = 0;
    private static final int INDEX_CACHE_MEMORY_LIMIT = 1024 * 1024;
    public static final int MAP_OUTPUT_INDEX_RECORD_LENGTH = 24;
    
    private List<CacheFile> bufList = new ArrayList<CacheFile>();

    private List<Integer> singleRecInds = new ArrayList<Integer>();
		MapSpiller(JobConf job,TaskAttemptID tid, TaskReporter rep) throws ClassNotFoundException {
			reporter = rep;
			conf = job;
			this.taskId = tid;
			mapOutputFile.setConf(conf);
			mapOutputByteCounter = reporter.getCounter(MAP_OUTPUT_BYTES);
			Counters.Counter combineInputCounter = 
	        reporter.getCounter(COMBINE_INPUT_RECORDS);
	      combineOutputCounter = reporter.getCounter(COMBINE_OUTPUT_RECORDS);
	      fileOutputByteCounter = reporter.getCounter(MAP_OUTPUT_MATERIALIZED_BYTES);
		// combiner
	    combinerRunner = CombinerRunner.create(conf, taskId, 
	                                           combineInputCounter,
	                                           reporter, null);
	    if (combinerRunner != null) {
	      combineCollector= new CombineOutputCollector(combineOutputCounter, reporter, conf);
	    } else {
	      combineCollector = null;
	    }			
			indexCacheList = new ArrayList<SpillRecord>();
			spilledRecordsCounter = reporter.getCounter(Counter.SPILLED_RECORDS);
		}
		public MapState getState() {
			return state;
		}
		public void setState(MapState s) {
			state = s;
		}
		public int getReadySpillNum() {
			return bufList.size();
		}

		
		private void mergeParts() throws IOException, InterruptedException, 
		ClassNotFoundException {
			// get the approximate size of the final output/index files
			long finalOutFileSize = 0;
			long finalIndexFileSize = 0;
			final Path[] filename = new Path[numSpills];
			final TaskAttemptID mapId = taskId;
			for(int i = 0; i < numSpilled; i++) {
				filename[i] = mapOutputFile.getSpillFile(i);
				finalOutFileSize += rfs.getFileStatus(filename[i]).getLen();
			}			
			for(Integer ssi : this.singleRecInds) {
				filename[ssi.intValue()] = mapOutputFile.getSpillFile(ssi.intValue());
				finalOutFileSize += rfs.getFileStatus(filename[ssi.intValue()]).getLen();
			}
			for(CacheFile cf : this.bufList) {
				finalOutFileSize += cf.getLen();
			}

			// 	read in paged indices
			for (int i = indexCacheList.size(); i < numSpills; ++i) {
				Path indexFileName = mapOutputFile.getSpillIndexFile(i);
				indexCacheList.add(new SpillRecord(indexFileName, conf, null));
			} 
			

			//make correction in the length to include the sequence file header
			//lengths for each partition
			finalOutFileSize += partitions * APPROX_HEADER_LENGTH;
			finalIndexFileSize = partitions * MAP_OUTPUT_INDEX_RECORD_LENGTH;
			Path finalOutputFile =
					mapOutputFile.getOutputFileForWrite(finalOutFileSize);
			Path finalIndexFile =
					mapOutputFile.getOutputIndexFileForWrite(finalIndexFileSize);

			//The output stream for the final single output file
			FSDataOutputStream finalFileOut = rfs.create(finalOutputFile, true, 4096);
			long cfCap =  CacheFile.size2Cap(finalOutFileSize);
			CacheFile finalCf = new CacheFile(CachePool.get(), cfCap ,finalFileOut, true, taskId, SpillScheduler.SORT);			
			FSDataOutputStream finalOut = new FSDataOutputStream(new CacheOutputStream(finalCf), null);
			if (numSpills == 0) {
				//	create dummy files
				IndexRecord rec = new IndexRecord();
				SpillRecord sr = new SpillRecord(partitions);
				try {
					for (int i = 0; i < partitions; i++) {
						long segmentStart = finalOut.getPos();
						Writer<K, V> writer =
								new Writer<K, V>(conf, finalOut, keyClass, valClass, codec, null);
						writer.close();
						rec.startOffset = segmentStart;
						rec.rawLength = writer.getRawLength();
						rec.partLength = writer.getCompressedLength();
						sr.putIndex(rec, i);
					}
					sr.writeToFile(finalIndexFile, conf);
				} finally {
					finalOut.close();
				}
				return;
			}
			{
				IndexRecord rec = new IndexRecord();
				final SpillRecord spillRec = new SpillRecord(partitions);
				for (int parts = 0; parts < partitions; parts++) {
					//	create the segments to be merged
					List<Segment<K,V>> segmentList =
							new ArrayList<Segment<K, V>>(numSpills);
					for(int i = 0; i < numSpills; i++) {
						IndexRecord indexRecord = indexCacheList.get(i).getIndex(parts);
						Segment s;
						if (i < numSpilled || this.singleRecInds.contains(new Integer(i))) {
							FSDataInputStream in =rfs.open(filename[i]);
							int off = (int)indexRecord.startOffset;						
							in.skip(off);
					
							Reader<K, V> reader = new Reader(conf, in, (int)indexRecord.partLength);
			        s = new Segment<K, V>(reader, true);			
						} else {
							int ind = i;
							if (this.singleRecInds.size()!=0) {
								for(Integer ssi : this.singleRecInds) {
									if (ssi.intValue() > i) {
										break;
									} else {
										ind--;
									}
								}
							}
							ind -= numSpilled;
							if (ind >= bufList.size()) {
								LOG.info(taskId + " mergePartsError!!! ");
							}
							CacheFile cf = this.bufList.get(ind);
							int off = (int)indexRecord.startOffset;							
							cf.reset();
							cf.skip(off);	
							Reader<K, V> reader = new Reader(conf, cf, (int)indexRecord.partLength);
			        s = new Segment<K, V>(reader, true);
						}
						segmentList.add(i, s);
						if (LOG.isDebugEnabled()) {
							LOG.debug("MapId=" + mapId + " Reducer=" + parts +
									"Spill =" + i + "(" + indexRecord.startOffset + "," +
									indexRecord.rawLength + ", " + indexRecord.partLength + ")");
						}
					}
					//		merge
					@SuppressWarnings("unchecked")
					RawKeyValueIterator kvIter = Merger.merge(conf, rfs,
							keyClass, valClass, codec,
							segmentList, segmentList.size(),
							new Path(mapId.toString()),
							conf.getOutputKeyComparator(), reporter,
							null, spilledRecordsCounter);										
					//write merged output to disk
					long segmentStart = finalOut.getPos();
					Writer<K, V> writer =
							new Writer<K, V>(conf, finalOut, keyClass, valClass, codec,
									spilledRecordsCounter);
					if (combinerRunner == null || numSpills < minSpillsForCombine) {
						rKVIter = kvIter;
						Merger.writeFile(kvIter, writer, reporter, conf);
						rKVIter = null;
					} else {
						combineCollector.setWriter(writer);
						combinerRunner.combine(kvIter, combineCollector);
					}
					writer.close();				
					rec.startOffset = segmentStart;
					rec.rawLength = writer.getRawLength();
					rec.partLength = writer.getCompressedLength();
					spillRec.putIndex(rec, parts);
				}
				finalOut.close();
				spillRec.writeToFile(finalIndexFile, conf);
				
				int total = 0;
				for(CacheFile cf : bufList) {
					total += cf.getCap();
					cf.clear();
				}				
				ramManager.unreserve(taskId, bufList.size(), total);		
				bufList.clear();
				for(int i = 0; i < numSpilled; i++) {
					rfs.delete(filename[i],true);
				} 
				for(Integer ssi : this.singleRecInds) {
					rfs.delete(filename[ssi.intValue()],true);
				}				
			}
			Path outputPath = mapOutputFile.getOutputFile();
	    fileOutputByteCounter.increment(rfs.getFileStatus(outputPath).getLen());	    
		}
		
		public void spillSingleRecord(K key, V val, int partition) throws IOException {
			LOG.info("spillSingleRecord ! ");
			 long size = MemoryElement.getBigMESize() + partitions * APPROX_HEADER_LENGTH;
	      FSDataOutputStream out = null;
	      try {
	        // create spill file
	        final SpillRecord spillRec = new SpillRecord(partitions);
	        final Path filename =
	            mapOutputFile.getSpillFileForWrite(numSpills, size);
	        out = rfs.create(filename);
	        
	        // we don't run the combiner for a single record
	        IndexRecord rec = new IndexRecord();
	        for (int i = 0; i < partitions; ++i) {
	          IFile.Writer<K, V> writer = null;
	          try {
	            long segmentStart = out.getPos();
	            // Create a new codec, don't care!
	            writer = new IFile.Writer<K,V>(conf, out, keyClass, valClass, codec,
	                                            spilledRecordsCounter);

	            if (i == partition) {
	              final long recordStart = out.getPos();
	              writer.append(key, val);
	              // Note that our map byte count will not be accurate with
	              // compression
	              mapOutputByteCounter.increment(out.getPos() - recordStart);
	            }
	            writer.close();

	            // record offsets
	            rec.startOffset = segmentStart;
	            rec.rawLength = writer.getRawLength();
	            rec.partLength = writer.getCompressedLength();
	            spillRec.putIndex(rec, i);

	            writer = null;
	          } catch (IOException e) {
	            if (null != writer) writer.close();
	            throw e;
	          }
	        }
	        if (totalIndexCacheMemory >= INDEX_CACHE_MEMORY_LIMIT) {	        	
	          // create spill index file
	          Path indexFilename =
	              mapOutputFile.getSpillIndexFileForWrite(numSpills, partitions
	                  * MAP_OUTPUT_INDEX_RECORD_LENGTH);
	          spillRec.writeToFile(indexFilename, conf);
	        } else {
	          indexCacheList.add(spillRec);
	          totalIndexCacheMemory +=
	            spillRec.size() * MAP_OUTPUT_INDEX_RECORD_LENGTH;
	        }
	        this.singleRecInds.add(new Integer(numSpills));
	        ++numSpills;	        
	      } finally {
	        if (out != null) out.close();
	      }
		}
		
		private void spill2Disk() throws IOException {
			if (bufList.size() == 0) {
				return;
			}
			FSDataOutputStream out = null;
			for (Integer ssi : this.singleRecInds) {
				if (numSpilled < ssi.intValue()) {
					break;
				}else	if (ssi.intValue() == numSpilled) {
					numSpilled++;
				} else {
					this.singleRecInds.remove(ssi);
				}
			}	
			CacheFile cf;;
			synchronized(bufList) {
				cf = this.bufList.remove(0);
			}
			LOG.info(taskId + " spill2Disk write begin ");
			long len = cf.getLen();
      final Path filename =
          mapOutputFile.getSpillFileForWrite(numSpilled, len);        
      out = rfs.create(filename);
      cf.writeFile(out);        
      LOG.info(taskId + " Finished spill to disk : " + numSpilled + " : " + len);
      numSpilled++;     	
      ramManager.unreserve(taskId, 1, cf.getCap());
      cf.clear();
		}
		
		private void sortAndSpill2Buffer() throws IOException, InterruptedException, ClassNotFoundException { //memEle内部已有序
			FSDataOutputStream out = null;
			MemoryElementQueue meQ = activeMemQueues.remove(taskId);
      try {
      	int size =  meQ.getSpillFileSize();      	
      	if (size % CacheUnit.cap > 0) {
      		size -=  size % CacheUnit.cap;
      		size += CacheUnit.cap;
      	}
      	long cfLen = CacheFile.size2Cap(size);
      	ramManager.reserve(taskId, cfLen);   
        final SpillRecord spillRec = new SpillRecord(partitions);     
        CacheFile cf = new CacheFile(CachePool.get(), cfLen);
        IndexRecord rec = new IndexRecord();
        out = new FSDataOutputStream(new CacheOutputStream(cf), null);
				List<Segment<K, V>> inMemorySegments = new ArrayList<Segment<K,V>>();			
				for(int i = 0; i < partitions; i++) {
					IFile.Writer writer = null;
					inMemorySegments.clear();
					meQ.createInMemorySegments(inMemorySegments);
					meQ.setPartition(i);
					RawKeyValueIterator rIter = Merger.merge(conf, rfs,
	            (Class<K>)conf.getMapOutputKeyClass(),
	            (Class<V>)conf.getMapOutputValueClass(),
	            inMemorySegments, inMemorySegments.size(),
	            new Path(taskId.toString()),
	            conf.getOutputKeyComparator(), reporter,
	            spilledRecordsCounter, null);

					try {
						long segmentStart = out.getPos();
						writer = new Writer(conf, out, keyClass, valClass);						
						if (combinerRunner == null) {
							rKVIter = rIter;
	            Merger.writeFile(rIter, writer, reporter, conf);
	            rKVIter = null;
	          } else {
	            combineCollector.setWriter(writer);
	            combinerRunner.combine(rIter, combineCollector);

	          }
	          writer.close();        	
            rec.startOffset = segmentStart;
            rec.rawLength = writer.getRawLength();
            rec.partLength = writer.getCompressedLength();      
            spillRec.putIndex(rec, i);
            writer = null;
          } finally {
            if (null != writer) writer.close();

          }
				}
				if (totalIndexCacheMemory >= INDEX_CACHE_MEMORY_LIMIT) {
          // create spill index file
          Path indexFilename =
              mapOutputFile.getSpillIndexFileForWrite(numSpills, partitions
                  * MAP_OUTPUT_INDEX_RECORD_LENGTH);
          spillRec.writeToFile(indexFilename, conf);
        } else {
          indexCacheList.add(spillRec);
          totalIndexCacheMemory +=
            spillRec.size() * MAP_OUTPUT_INDEX_RECORD_LENGTH;
        }			
				ramManager.closeInMemorySpill(size);
				synchronized(bufList) {
        	bufList.add(cf);
        }
        LOG.info(taskId + " Finished spill to buffer : " + numSpills);
        ++numSpills;
      } finally {
        if (out != null) out.close();
        meQ.close2Recycle();
      }
		}
		public void kill() {			
			if (rKVIter != null) {				
					rKVIter.stop();
			}
			int total = 0;
			for(CacheFile cf : bufList) {
				total += cf.getCap();
				cf.clear();
			}			
			ramManager.unreserve(taskId, bufList.size(), total);
			bufList.clear();
			isKilled = true;
		}
	}
	
	class SpillThread extends Thread {
		
		private boolean isBusy = false;

		private IntWritable index = new IntWritable(0);
		private TaskAttemptID currentQue = null;
		private Object free = new Object(); 
		public boolean isBusy() {
			return isBusy;
		}		
		public void toSpillTaskRemoved(int i) {
			if (i == -1) {
				return;
			}
			int tNum = toSpillMemQInds.size();
			synchronized (index) {				
				if (tNum == 0) {
					index.set(0);
				} else if (i < index.get()) {					
					index.set((index.get() - 1 + tNum) % tNum);
				}	else if (i == index.get()) {
					index.set(index.get()%tNum);
				}
			}
		}
		
		public TaskAttemptID getCurrentSpillId() {
			return currentQue;
		}		
		
		private TaskAttemptID getNextSpillQue() {			
			synchronized (toSpillMemQInds) {
				synchronized(index) {

					int tNum = toSpillMemQInds.size();
					for (int i = index.get(); i < index.get() + tNum; i++) {						
						TaskAttemptID tid = toSpillMemQInds.get(i%tNum);									
						if (mapSpillers.get(tid).getReadySpillNum() > 0 &&
								mapSpillers.get(tid).getState().compareTo(MapState.spill) == 0) {
							index.set((i + 1) % tNum);							
							return tid;
						}
					}					
					return null;
				}								
			}
		}		
		public void waitForFree() throws InterruptedException {
			synchronized (free) {
				free.wait();
			}
		}
		public void run() {			
			while (true) {					
				try {	
					isBusy = false;
					ramManager.waitForDataToSpill();
					isBusy = true;
				}	catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					break;
				}
				currentQue = getNextSpillQue();		
				try {					
					if(currentQue == null || mapSpillers.get(currentQue).getState().compareTo(MapState.merge) == 0) {
						continue;
					}
					mapSpillers.get(currentQue).spill2Disk();
					synchronized (free) {
						free.notify();
					}					
					currentQue = null;
				} catch (Throwable t) {
					String logMsg = "Task " + currentQue + " failed : " 
							+ StringUtils.stringifyException(t);
					Child.getTask(currentQue).reportFatalError(currentQue, t, logMsg);
				} 
			}
		}
	}
			
	
	class MemoryElementQueue<K,V> {
		private final TaskAttemptID taskId;
		private int memSize = 0;
		private int priority = 0;
		private int spillFileSize = 0;
		private List<MemoryElement> memElementQueue = new ArrayList<MemoryElement>();
		public MemoryElementQueue(TaskAttemptID tId) {
			taskId = tId;
		}
		public void enQueue(MemoryElement me) {
			if (!me.getTaskID().equals(taskId)) {
				return;
			}
			this.memElementQueue.add(me);
			if (me.isBig()) {
				priority = 1;
			}
			memSize += me.getSize();
			spillFileSize += me.getSpillFileSize();
			if (memSize >= perQueToSpillUpLineMemSize || 
					memElementQueue.size() >= perQueToSpillUpLineMENum ){
				startSpillQueue(this.taskId);
				return;
			}
		}
		
		public void startSpill() {
			for (MemoryElement me : this.memElementQueue) {
				me.startSpill();
			}
		}
	
		public TaskAttemptID getTaskId() {
			return this.taskId;
		}
		
		public void close2Recycle() {
			for (MemoryElement me : this.memElementQueue) {
				me.reset();	
				recycleMemElement(me);			
			}
			this.memElementQueue.clear();
		}
		
		public void createInMemorySegments(
				List<Segment<K,V>> inMemorySegments)
						throws IOException {			
      for (MemoryElement me : this.memElementQueue) {
        Reader reader = 
          new IFile.RawMemoryReader(me);
        Segment segment = 
          new Segment(reader, true);
        inMemorySegments.add(segment);
      }
		}
		
		public void setPartition(int p) throws IOException {
			for (MemoryElement me : this.memElementQueue) {
				me.setCurSpillPartition(p);
			}
		}
		
		public int getPriority() {
			return this.priority;
		}
		
		public int getMemSize() {
			return memSize;
		}
		public int getSpillFileSize() {
			//(EOF+checksum) * partitions
			return spillFileSize + (WritableUtils.getVIntSize(-1) * 2 ) * partitions;
		}
		
		public boolean hasReg() {
			for (MemoryElement me : this.memElementQueue) {
				if (!me.isBig()) {
					return true;
				}
			}
			return false;
		}
		public int getQueueSize() {
			return memElementQueue.size();
		}
		
		public List<MemoryElement> getQueue() {
			return this.memElementQueue;
		}	
	}
	
	private JobConf conf = null;
	private int numJvmSlots;

	//threadhold of spilling
	private long perQueToSpillUpLineMemSize;
	private int perQueToSpillUpLineMENum;   
	
	private Map<TaskAttemptID, MemoryElementQueue> activeMemQueues = 
			new ConcurrentHashMap<TaskAttemptID, MemoryElementQueue>();

	private Map<TaskAttemptID, MapSpiller> mapSpillers = 
			new ConcurrentHashMap<TaskAttemptID, MapSpiller>();
	private List<TaskAttemptID> toSpillMemQInds =
			new ArrayList<TaskAttemptID>();         //for polling 
	private SpillThread spillThread = new SpillThread();
	
	private List<MemoryElement> regMemElePool = new LinkedList<MemoryElement>();

				
	private  int partitions;
	private  FileSystem localFs;
  private  FileSystem rfs;
  private  Class keyClass;
  private  Class valClass;
//Compression for map-outputs
  private CompressionCodec codec = null;
  private  int minSpillsForCombine;
    
  private final static int APPROX_HEADER_LENGTH = 150;
  private static final Log LOG = LogFactory.getLog(DefaultJvmMemoryManager.class.getName());
  
  private DefaultMapRamManager ramManager;

	DefaultJvmMemoryManager() throws IOException {		
				
	}
	
	private void initialize() throws IOException {
		numJvmSlots = conf.getInt("mapred.tasktracker.map.tasks.maximum", 2);
		final float recper = conf.getFloat("io.sort.record.percent",(float)0.05);
    final int regSortkb = conf.getInt("io.sort.element.kb", 1024);      
    long regSortSize = regSortkb << 10;  	
    
  	int perQueToSpillUpLineMemSizemb = conf.getInt("io.spill.upline.mb", 24);
  	final int bigSortmb = conf.getInt("io.sort.big.element.mb", perQueToSpillUpLineMemSizemb);
  	this.perQueToSpillUpLineMemSize =perQueToSpillUpLineMemSizemb << 20;  	
  
  	long regEleNum = (perQueToSpillUpLineMemSize / (int)((regSortSize) * (1 - recper)) + 2) * this.numJvmSlots;
  	perQueToSpillUpLineMENum = (int)regEleNum / this.numJvmSlots -1;    
    MemoryElement.initMemElements(recper, regSortkb, bigSortmb);    
    for (int i = 0; i < regEleNum; i++) {
    	this.regMemElePool.add(new MemoryElement(false));
    }    
    LOG.info("total regular pool size: " + regEleNum * regSortSize);
    long maxBufSize = conf.getLong("mapred.child.buf.total.bytes",
        Runtime.getRuntime().maxMemory() - regEleNum * regSortSize);
    float maxBufUsePer = conf.getFloat("mapred.child.buf.percent", 0.7f);
    long maxTotal = (long)(maxBufSize * maxBufUsePer);
    ChildRamManager.init(maxTotal, conf);
    int cacheUnitCap = conf.getInt("mapred.cache.unit.kb", 2048) << 10;
    CachePool.init(maxTotal, cacheUnitCap);
    ramManager = new DefaultMapRamManager(conf, ChildRamManager.get());
    spillThread.setDaemon(true);
    spillThread.setName("SpillThread");    
    spillThread.start();    
    SpillScheduler.get().start();
	}
	
	public JobConf getConf() {
		return this.conf;
	}
	
	public void setConf(JobConf job) throws IOException {
		this.conf = job;
		keyClass = (Class)conf.getMapOutputKeyClass();
    valClass = (Class)conf.getMapOutputValueClass();
 // compression
    if (conf.getCompressMapOutput()) {
      Class<? extends CompressionCodec> codecClass =
        conf.getMapOutputCompressorClass(DefaultCodec.class);
      codec = ReflectionUtils.newInstance(codecClass, conf);
    }
    minSpillsForCombine = conf.getInt("min.num.spills.for.combine", 3);
    
		localFs = FileSystem.getLocal(conf);
    partitions = conf.getNumReduceTasks();     
    rfs = ((LocalFileSystem)localFs).getRaw();
    initialize();
	}
	
	public MemoryElement getRegMemoryElement() {
		synchronized (regMemElePool) {			
			while (regMemElePool.size() == 0) {				
					try {						
						regMemElePool.wait();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						return null;
					}				
			}		
			return regMemElePool.remove(0);
		}
	}	
	
	
	public MemoryElement getBigMemoryElement(TaskAttemptID tid) throws InterruptedException {
		if(!ramManager.tryReserve(MemoryElement.getBigMESize())) {
			ramManager.reserve(tid, MemoryElement.getBigMESize());
			return new MemoryElement(true);
		}
		return null;
	}
	
	private void startSpillQueue(TaskAttemptID tid) {
		if(this.activeMemQueues.get(tid) == null) {
			LOG.info("startSpillQueue : the queue to be spilled is null.");
			return;		
		}
		MemoryElementQueue meq;				
		meq = this.activeMemQueues.get(tid);
		meq.startSpill();		
		try {			
			mapSpillers.get(tid).sortAndSpill2Buffer();			
		} catch (Throwable t) {
			String logMsg = "Task " + tid + " failed : " 
					+ StringUtils.stringifyException(t);
			Child.getTask(tid).reportFatalError(tid, t, logMsg);
		} 
	}
	
	public void returnMemElement(MemoryElement me, boolean isEmpty) {
		if(me==null) {
			return;
		}
		if(isEmpty) {
			synchronized (this.regMemElePool) {				
				this.regMemElePool.add(me);
				this.regMemElePool.notify();				
			}
			return;
		}		
		if (activeMemQueues.get(me.getTaskID()) == null) {
			activeMemQueues.put(me.getTaskID(), new MemoryElementQueue(me.getTaskID()));
		}	
		activeMemQueues.get(me.getTaskID()).enQueue(me);
		if(me.isBig()) {
			ramManager.closeInMemorySpill(me.getBigMESize());
		}		
	}
		
	public void recycleMemElement(MemoryElement me) {
		
		if (!me.isBig()) {
			synchronized (this.regMemElePool) {				
				this.regMemElePool.add(me);
				this.regMemElePool.notify();				
			}
		} else {
			ramManager.unreserve(me.getTaskID(), 1, MemoryElement.getBigMESize());
		}
	}
	public void registerSpiller(JobConf job, TaskAttemptID tid, 
			TaskReporter reporter) throws ClassNotFoundException {
			this.mapSpillers.put(tid, new MapSpiller(job, tid, reporter));
			synchronized (this.toSpillMemQInds) {
				this.toSpillMemQInds.add(tid);
			}
			this.ramManager.registerMap(tid);
	}
	public void spillSingleRecord(TaskAttemptID tid, final Object key, final Object value,
      int partition) throws IOException, InterruptedException {		
		mapSpillers.get(tid).spillSingleRecord(key, value, partition);				
	}
	
	public void flush(TaskAttemptID tid) 
			throws InterruptedException, IOException, ClassNotFoundException {
		if (this.activeMemQueues.get(tid) != null) {
			startSpillQueue(tid);
		}	
		if (spillThread.isBusy() && tid.equals(spillThread.getCurrentSpillId())) {
			spillThread.waitForFree();
		}
		mapSpillers.get(tid).setState(MapState.merge);		
		mapSpillers.get(tid).mergeParts();
		if (mapSpillers.get(tid).isKilled) {
			return;
		}
		synchronized (this.toSpillMemQInds) {			
			spillThread.toSpillTaskRemoved(this.toSpillMemQInds.indexOf(tid));				
			this.toSpillMemQInds.remove(tid);		
		}
		this.mapSpillers.remove(tid);
		this.ramManager.unregisterMap(tid);						
	}	
	public void kill(TaskAttemptID tid) {
		if (!mapSpillers.containsKey(tid)) {
			return;
		}
		mapSpillers.get(tid).kill();
		ramManager.unregisterMap(tid);
		mapSpillers.remove(tid);
		MemoryElementQueue meq = activeMemQueues.remove(tid);
		if (meq != null) {
			if (meq.getQueueSize() > 0) {
				meq.close2Recycle();
			}
		}
		synchronized (this.toSpillMemQInds) {			
			spillThread.toSpillTaskRemoved(this.toSpillMemQInds.indexOf(tid));				
			this.toSpillMemQInds.remove(tid);		
		}
	}
}
