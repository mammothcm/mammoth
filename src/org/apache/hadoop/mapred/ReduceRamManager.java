package org.apache.hadoop.mapred;

import java.io.InputStream;

abstract class ReduceRamManager implements RamManager{
	static long maxSize = 0;
//	static long sortLimit = 0;
	/**
   * Reserve memory for data coming through the given input-stream.
   * 
   * @param requestedSize size of memory requested
   * @param in input stream
   * @throws InterruptedException
   * @return <code>true</code> if memory was allocated immediately, 
   *         else <code>false</code>
   */
	abstract boolean reserve(Thread t, long requestedSize, InputStream in) 
  throws InterruptedException;
  
  /**
   * Return memory to the pool.
   * 
   * @param requestedSize size of memory returned to the pool
   */
  abstract void unreserve(long requestedSize);
  
 // void spill2Disk(int size);
  
  abstract TaskAttemptID getTaskId();

	abstract void merge2Disk(boolean memOrDisk, long size);
	
	static void setMaxMem(long l) {
		maxSize = l;
	}	
/*	static void setSortLimitMem(long size) {
		sortLimit = size;
	}
	static void setLimit(long max, long sortMax) {
		maxSize = max;
		//sortLimit = sortMax;
	}*/
}
