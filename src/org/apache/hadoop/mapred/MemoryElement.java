package org.apache.hadoop.mapred;


import static org.apache.hadoop.mapred.Task.Counter.MAP_OUTPUT_BYTES;
import static org.apache.hadoop.mapred.Task.Counter.MAP_OUTPUT_RECORDS;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableUtils;

import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;

import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.IndexedSorter;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.QuickSort;
import org.apache.hadoop.util.ReflectionUtils;

public class MemoryElement 
	implements IndexedSortable{	
	public static class MapBufferTooSmallException extends IOException {
    public MapBufferTooSmallException(String s) {
      super(s);
    }
  }
	public static class MemoryElementFullException extends IOException {
    public MemoryElementFullException(String s) {
      super(s);
    }
  }
  protected class BlockingBuffer extends DataOutputStream {

    public BlockingBuffer() {
      this(new Buffer());
    }

    private BlockingBuffer(OutputStream out) {
      super(out);
    }

    /**
     * Mark end of record. Note that this is required if the buffer is to
     * cut the spill in the proper place.
     */
    public int markRecord() {
      bufmark = bufindex;
      return bufindex;
    }
    
  }

  public class Buffer extends OutputStream {
    private final byte[] scratch = new byte[1];

    @Override
    public synchronized void write(int v)
        throws IOException {
      scratch[0] = (byte)v;
      write(scratch, 0, 1);
    }

    /**
     * Attempt to write a sequence of bytes to the collection buffer.
     * This method will block if the spill thread is running and it
     * cannot write.
     * @throws MapBufferTooSmallException if record is too large to
     *    deserialize into the collection buffer.
     */
    @Override
    public synchronized void write(byte b[], int off, int len)
        throws IOException {
      boolean buffull = false;
      
    	buffull = bufindex + len > bufvoid;
    	if (buffull) {       			
    		if ( kvend != kvindex ) {
    			//We have buffered records
    			//LOG.info("cm ..... write Spilling map output: buffer size= " + bufmark);
    			throw new MemoryElementFullException(taskId + " : " + bufmark);
    		}
      	else {
          // We have no buffered records, and this record is too large
          // to write into kvbuffer. We must spill it directly from
          // collect
          final int size = ((bufend <= bufindex)
            ? bufindex - bufend
            : (bufvoid - bufend) + bufindex) + len;
       //   reset();
          throw new MapBufferTooSmallException(size + " bytes");  
        }
      }      
      // here, we know that we have sufficient space to write      
      System.arraycopy(b, off, kvbuffer, bufindex, len);
      bufindex += len;    
    }
  }
  
  protected class InMemValBytes extends DataInputBuffer {
    private byte[] buffer;
    private int start;
    private int length;
          
    public void reset(byte[] buffer, int start, int length) {
      this.buffer = buffer;
      this.start = start;
      this.length = length;
      
      if (start + length > bufvoid) {
        this.buffer = new byte[this.length];
        final int taillen = bufvoid - start;
        System.arraycopy(buffer, start, this.buffer, 0, taillen);
        System.arraycopy(buffer, 0, this.buffer, taillen, length-taillen);
        this.start = 0;
      }
      
      super.reset(this.buffer, this.start, this.length);
    }
  }

  protected class MRResultIterator implements RawKeyValueIterator {
    private final DataInputBuffer keybuf = new DataInputBuffer();
    private final InMemValBytes vbytes = new InMemValBytes();
    private int end;
    private int current;
    public MRResultIterator(int start, int end) {
      this.end = end;
      current = start - 1;
    }
    public boolean next() throws IOException {
      return ++current < end;
    }
    public void set(int start, int end) {
    	this.end = end;
    	this.current = start - 1;
    }
    public DataInputBuffer getKey() throws IOException {
      final int kvoff = kvoffsets[current % kvoffsets.length];
      keybuf.reset(kvbuffer, kvindices[kvoff + KEYSTART],
                   kvindices[kvoff + VALSTART] - kvindices[kvoff + KEYSTART]);
      LOG.info("getKey " + this.hashCode());
     // LOG.info("getKey : " +kvindices[kvoff + KEYSTART] + " : " + (kvindices[kvoff + VALSTART] - kvindices[kvoff + KEYSTART]));
      return keybuf;
    }
    public DataInputBuffer getValue() throws IOException {
      getVBytesForOffset(kvoffsets[current % kvoffsets.length], vbytes);
      return vbytes;
    }
    
    public void getKey(DataInputBuffer in) throws IOException {
      final int kvoff = kvoffsets[current];
      in.reset(kvbuffer, kvindices[kvoff + KEYSTART],
                   kvindices[kvoff + VALSTART] - kvindices[kvoff + KEYSTART]);
      //LOG.info("getKey " + this.hashCode());
     // LOG.info("getKey : " + current + " : " + kvoff + " : " 
     // 		+ kvindices[kvoff + KEYSTART] + " : " + (kvindices[kvoff + VALSTART] - kvindices[kvoff + KEYSTART]));      
    }
    public void getValue(DataInputBuffer in) throws IOException {
    	final int kvoff = kvoffsets[current];
    	final int vstart = kvindices[kvoff + VALSTART];
     // getVBytesForOffset(kvoffsets[current % kvoffsets.length], vbytes);
    	int vlen = kvoff / ACCTSIZE == (kvend - 1) ? 
    			bufmark - vstart:    				
    				kvindices[kvoff + ACCTSIZE + KEYSTART] - vstart;
      in.reset(kvbuffer, vstart, vlen);      
    //  if (taskId.toString().substring(32, 34).equals("11"))
    //  	LOG.info("getValue : " + current+ " : " + kvoffsets[current] + " : " 
    //  			+ vstart + " : " + vlen);
    }
    public Progress getProgress() {
      return null;
    }
    public void close() { }
		@Override
		public void stop() {
			// TODO Auto-generated method stub
			
		}
  }
  
	//private JobConf conf;
	private static float recper;
  private static int regSortkb;
  private static int bigSortmb;
  private static final Log LOG = LogFactory.getLog(MemoryElement.class.getName());
  private int partitions;
  private JobConf job;
  private TaskReporter reporter;
  private Class keyClass;
  private Class valClass;
  private RawComparator comparator;
  private SerializationFactory serializationFactory;
  private Serializer keySerializer;
  private Serializer valSerializer;
 // private final CombinerRunner<K,V> combinerRunner;
 // private final CombineOutputCollector<K, V> combineCollector;
  
  // Compression for map-outputs
//  private CompressionCodec codec = null;

  // k/v accounting
  private volatile int kvstart = 0;  // marks beginning of spill
  private volatile int kvend = 0;    // marks beginning of collectable
  private int kvindex = 0;           // marks end of collected
  private final int[] kvoffsets;     // indices into kvindices
  private final int[] kvindices;     // partition, k/v offsets into kvbuffer
  private volatile int bufstart = 0; // marks beginning of spill
  private volatile int bufend = 0;   // marks beginning of collectable
  private volatile int bufvoid = 0;  // marks the point where we should stop
                                     // reading at the end of the buffer
  private int bufindex = 0;          // marks end of collected
  private int bufmark = 0;           // marks end of record
  private byte[] kvbuffer;           // main output buffer
  private static final int PARTITION = 0; // partition offset in acct
  private static final int KEYSTART = 1;  // key offset in acct
  private static final int VALSTART = 2;  // val offset in acct
  private static final int ACCTSIZE = 3;  // total #fields in acct
  private static final int RECSIZE =
                     (ACCTSIZE + 1) * 4;  // acct bytes per record

  private int totalLenVIntSize = 0;
  
  private final BlockingBuffer bb = new BlockingBuffer();

  private IndexedSorter sorter;
 
  private  Counters.Counter mapOutputByteCounter;
  private  Counters.Counter mapOutputRecordCounter;

  private TaskAttemptID taskId;
  private boolean big = false;  
 
  private MRResultIterator spillKvIter = new MRResultIterator(0, 0);
  
  private int[] partitionInd = null;
  public static int getBigMESize() {
  	return bigSortmb << 20;
  }
  public static void initMemElements(float recordPercent, int regSortkbElement, int bigSortmbElement) throws IOException {
  	recper = recordPercent;
  	regSortkb = regSortkbElement;
  	bigSortmb = bigSortmbElement;
  	if (recper > (float)1.0 || recper < (float)0.01) {
      throw new IOException("Invalid \"io.sort.record.percent\": " + recper);
    }
    if ((regSortkb & 0x7FFF) != regSortkb) {
      throw new IOException("Invalid \"io.sort.kb\": " + regSortkb);
    }
    if ((bigSortmb & 0x7FF) != bigSortmb) {
      throw new IOException("Invalid \"io.sort.mb\": " + bigSortmb);
    }
  }
  MemoryElement(boolean isbig) {
  	this.big = isbig;
  	int maxMemUsage = big ? bigSortmb << 20 : regSortkb << 10;  	
    int recordCapacity = (int)(maxMemUsage * recper);
    recordCapacity -= recordCapacity % RECSIZE;
    kvbuffer = new byte[maxMemUsage - recordCapacity];
    bufvoid = kvbuffer.length;
    recordCapacity /= RECSIZE;
    kvoffsets = new int[recordCapacity];
    kvindices = new int[recordCapacity * ACCTSIZE];
    //for merge. Record each partition's (start, end)
    
  //  LOG.info("data buffer = " +  kvbuffer.length);
  //  LOG.info("record buffer = " +  kvoffsets.length);    
  }
  
  public void initialize(JobConf job, TaskReporter reporter, TaskAttemptID taskId) throws ClassNotFoundException, IOException{
  	this.reporter = reporter;     
    this.taskId = taskId;
    mapOutputByteCounter = reporter.getCounter(MAP_OUTPUT_BYTES);
    mapOutputRecordCounter = reporter.getCounter(MAP_OUTPUT_RECORDS);    
    this.job = job;
    
    sorter = ReflectionUtils.newInstance(
        job.getClass("map.sort.class", QuickSort.class, IndexedSorter.class), job);
    partitions = job.getNumReduceTasks();
    if (partitionInd == null || partitions * 2 != partitionInd.length) {
    	partitionInd = new int[partitions * 2];
    }
    comparator = job.getOutputKeyComparator();
    keyClass = (Class)job.getMapOutputKeyClass();
    valClass = (Class)job.getMapOutputValueClass();
    serializationFactory = new SerializationFactory(job);
    keySerializer = serializationFactory.getSerializer(keyClass);
    keySerializer.open(bb);
    valSerializer = serializationFactory.getSerializer(valClass);
    valSerializer.open(bb);
    reset();
  }
  
  public TaskAttemptID getTaskID() {
  	return this.taskId;
  }
  
  public boolean isBig() {
  	return big;
  }
  
  public int getSize() {
  	return bufmark;
  }
  public int getSpillFileSize() {
  	return bufmark + this.totalLenVIntSize;
  }
  public void reset() {
  	bufstart = bufend = bufindex = bufmark = 0;
    kvstart = kvend = kvindex = 0;
    bufvoid = kvbuffer.length;
    totalLenVIntSize = 0;
  }
  
  public synchronized void collect(Object key, Object value, int partition  
      ) throws IOException {            //true:成功，false：失败
  	
  	final int kvnext = (kvindex + 1) % kvoffsets.length;
  	boolean kvfull;  		
  	kvfull = kvnext == kvstart;  		
  	if (kvfull) {  				
   		throw new MemoryElementFullException(taskId + " : " + bufmark);
  	}
		// 	serialize key bytes into buffer
		int keystart = bufindex;
		keySerializer.serialize(keyClass.cast(key));
		this.totalLenVIntSize += WritableUtils.getVIntSize(bufindex - keystart);
		// 	serialize value bytes into buffer
		final int valstart = bufindex;
		valSerializer.serialize(valClass.cast(value));
		int valend = bb.markRecord();
		this.totalLenVIntSize += WritableUtils.getVIntSize(valend - valstart);
		if (partition < 0 || partition >= partitions) {
			throw new IOException("Illegal partition for " + key + " (" +
					partition + ")");
		}

		mapOutputRecordCounter.increment(1);
		mapOutputByteCounter.increment(valend - keystart);

		// update accounting info
		int ind = kvindex * ACCTSIZE;
		kvoffsets[kvindex] = ind;
		kvindices[ind + PARTITION] = partition;
		kvindices[ind + KEYSTART] = keystart;
		kvindices[ind + VALSTART] = valstart;
		kvindex = kvnext;  	
  }
  
  public synchronized void startSpill() {
  //  LOG.info("bufstart = " + bufstart + "; bufend = " + bufmark +
 //            "; bufvoid = " + bufvoid);
  //  LOG.info("kvstart = " + kvstart + "; kvend = " + kvindex +
 //            "; length = " + kvoffsets.length);
  	//LOG.info("me startSpill : " + bufmark + " : " + kvindex);
    kvend = kvindex;
    bufend = bufmark;
    sort();    
    calPartitionBounds();
  }
  
  private void calPartitionBounds() {
  	int spindex = 0;  	
  	int pid = 0;
  	do {  		
  		int partition = kvindices[kvoffsets[spindex % kvoffsets.length] + PARTITION];
  		for (; pid < partition; pid++) {
  			partitionInd[pid * 2] = 0;
  			partitionInd[pid * 2 + 1] = 0;
  		}
  		partitionInd[partition * 2] = spindex;
  		while (spindex < kvend &&
          kvindices[kvoffsets[spindex % kvoffsets.length]
                    + PARTITION] == partition) {
        ++spindex;
      }  		
  		partitionInd[partition * 2 + 1] = spindex;
  		pid = partition + 1;
  	} while(spindex < kvend);    
  	for (; pid < partitions; pid++) {
  		partitionInd[pid * 2] = 0;
  		partitionInd[pid * 2 + 1] = 0;
  	}  	
  }
  
  public int compare(int i, int j) {
    final int ii = kvoffsets[i % kvoffsets.length];
    final int ij = kvoffsets[j % kvoffsets.length];
    // sort by partition
    if (kvindices[ii + PARTITION] != kvindices[ij + PARTITION]) {
      return kvindices[ii + PARTITION] - kvindices[ij + PARTITION];
    }
    // sort by key
    return comparator.compare(kvbuffer,
        kvindices[ii + KEYSTART],
        kvindices[ii + VALSTART] - kvindices[ii + KEYSTART],
        kvbuffer,
        kvindices[ij + KEYSTART],
        kvindices[ij + VALSTART] - kvindices[ij + KEYSTART]);
  }

  public void sort() {
  	sorter.sort(MemoryElement.this, kvstart, kvend, reporter);
  }
  /**
   * Swap logical indices st i, j MOD offset capacity.
   * @see IndexedSortable#swap
   */
  public void swap(int i, int j) {
    i %= kvoffsets.length;
    j %= kvoffsets.length;
    int tmp = kvoffsets[i];
    kvoffsets[i] = kvoffsets[j];
    kvoffsets[j] = tmp;
  }
  
  /**
   * Given an offset, populate vbytes with the associated set of
   * deserialized value bytes. Should only be called during a spill.
   */
  private void getVBytesForOffset(int kvoff, InMemValBytes vbytes) {
    final int nextindex = (kvoff / ACCTSIZE ==
                          (kvend - 1 + kvoffsets.length) % kvoffsets.length)
      ? bufend
      : kvindices[(kvoff + ACCTSIZE + KEYSTART) % kvindices.length];
    int vallen = (nextindex >= kvindices[kvoff + VALSTART])
      ? nextindex - kvindices[kvoff + VALSTART]
      : (bufvoid - kvindices[kvoff + VALSTART]) + nextindex;
    vbytes.reset(kvbuffer, kvindices[kvoff + VALSTART], vallen);
  }
  
  public void setCurSpillPartition(int p) throws IOException {
  	//IntWritable start = new IntWritable(0);
  	//IntWritable end = new IntWritable(0);
  	//this.getBufferBoundries(p, start, end);
  	if (spillKvIter != null) {  
				spillKvIter.close();			
  	}
  	//if (this.taskId.toString().substring(32, 34).equals("11"))
  	//LOG.info("setCurSpillPartition "+ start+ " : " + end + " : "+ p + " : " +
  	//		bufindex + " : " + kvindex + " : " + bufend + " : " + kvend);
  	
  	spillKvIter.set(partitionInd[p * 2], partitionInd[p * 2 + 1]);
  }
  
  private void getBufferBoundries(int partition, IntWritable start, IntWritable end) {
  	int spindex = 0;
    while (spindex < kvend &&
        kvindices[kvoffsets[spindex % kvoffsets.length]
                  + PARTITION] != partition) {
      ++spindex;
    }
    start.set(spindex);
    while (spindex < kvend &&
        kvindices[kvoffsets[spindex % kvoffsets.length]
                  + PARTITION] == partition) {
      ++spindex;
    }
    end.set(spindex);
    //if (this.taskId.toString().substring(32, 34).equals("11"))
   // if (partition == 1) 
    //	LOG.info("getBufferBoundries : "+ partition+ " : " + start + " : " + end);
  }
  
  public boolean nextKeyValue(DataInputBuffer key, DataInputBuffer val) throws IOException {
  	//LOG.info("nextKeyValue" + this.hashCode());
  	if (!spillKvIter.next()) {
  		return false;
  	} else {
  		//LOG.info("nextKeyValue : " + spillKvIter.getKey().getData().length +
  		//		" : " + spillKvIter.getKey().getPosition() +
  		//		" : " +	spillKvIter.getKey().getLength());
  		//DataInputBuffer tk = spillKvIter.getKey();
  		//DataInputBuffer tv = spillKvIter.getValue();
  		//key.reset(tk.getData(), tk.getPosition(), tk.getLength());
  		//val.reset(tv.getData(),  tv.getPosition(),  tv.getLength());
  		spillKvIter.getKey(key);
  		spillKvIter.getValue(val);
  		return true;
  	}
  }
  
  public void dumpToFile(FileOutputStream out) throws IOException {
  	out.write(kvbuffer, bufstart, bufend);
    out.close();
  }
}