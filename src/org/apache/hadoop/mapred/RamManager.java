/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred;

import java.io.InputStream;

/**
 * <code>RamManager</code> manages a memory pool of a configured limit.
 */
public interface RamManager {	
	void await() throws InterruptedException;  //阻塞该线程
	void awake(); //将所需内存分配给所需线程，唤醒该线程
	void schedule(); //下级内存管理器重新调度内存请求队列。
  long getReservedSize();  //下级内存管理器已经保留的内存总量 
  long getToReserveSize();  //获取下级内存管理器调度得到的下一个分配内存的请求的大小。
  boolean isSpilling();
  long getFullSize();
}
