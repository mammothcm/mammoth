package org.apache.hadoop.mapred;

import java.io.InputStream;

abstract class ReduceRamManager implements RamManager{
	protected long maxSize = 0;
	protected long toSpillSize = -1;
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
  
  abstract long getTotalShuffleSize();
  
  abstract long getSpilledSize();
  
  abstract TaskAttemptID getTaskId();
  
  abstract boolean isClosed();

	abstract void merge2Disk(boolean memOrDisk, long size);
	
	public void setMaxMem(long l) {
		maxSize = l;
	}
	public void set2SpillSize(long l) {
		toSpillSize = l;
	}	
	abstract public long getDesiredSize();
}
