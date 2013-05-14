package org.apache.hadoop.mapred;

interface MapRamManager extends RamManager{
	int getMapNum();
	long getMapHeap();
	void spill2Disk();
}
