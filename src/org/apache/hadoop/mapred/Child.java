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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;

import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.mapreduce.server.tasktracker.JVMInfo;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetricsSource;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.LogManager;

/** 
 * The main() for child processes. 
 */

class Child {
	
	//*******************memory management*********************
	
	
	//************task manageMent*************************
	
	private List<JvmTask> pendingTasks = new ArrayList<JvmTask>();
	
	private static Map<TaskAttemptID, RunningTask> runningTasks = 
			new ConcurrentHashMap<TaskAttemptID, RunningTask>();	
	private static Map<TaskAttemptID, RunningTask> cleanupTasks = 
			new ConcurrentHashMap<TaskAttemptID, RunningTask>();	
	private GetNewTaskThread getNewTaskThread = new GetNewTaskThread();
	
  private LaunchTaskThread launchTaskThread = new LaunchTaskThread();
  

  private static DefaultJvmMemoryManager jvmMemManager;
  
	private static TaskUmbilicalProtocol umbilical;
	private static JvmContext jvmContext;
	private static String logLocation;
	private static JobConf defaultConf = new JobConf();
	private static JobConf firstConf = null;
	private static Token<JobTokenIdentifier> jt;
	private static Object finishSignal = new Object();
	
	public Child() {
		try {
		
			jvmMemManager = new DefaultJvmMemoryManager(umbilical.getMaxCurrentMapTasks());
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.error("jvmMemoryManager construct failed", e);			
		}
	}
	//private 
	private class GetNewTaskThread extends Thread{
		private static final long SLEEP_TIME = 1000;		
		final int SLEEP_LONGER_COUNT = 5;
		private boolean isEnded = false;
		void end() {
			this.interrupt();
			this.isEnded = true;
		}
		public void run() {
			int idleLoopCount = 0;
			do {
				try {
					JvmTask myTask;					
					myTask = umbilical.getTask(jvmContext);					
					if (myTask.shouldDie()) {
						String tmp = "";
						for(TaskAttemptID id : runningTasks.keySet()) {
							tmp += (" " + id);
							//Child.killTask(id);
						}
						LOG.info("jvm should die ,runningTasks : " + tmp);
						System.exit(0);						
						/*while (runningTasks.size() > 0) {
							synchronized (runningTasks) {
								runningTasks.wait();
							}
						}
						LOG.info("runningTasks waited");
						synchronized(finishSignal) {
							finishSignal.notifyAll();
						}
						break;*/
					}
		            
					if (myTask.getTask() == null) {
						if (++idleLoopCount >= SLEEP_LONGER_COUNT) {
							//we sleep for a bigger interval when we don't receive
							//tasks for a while
							Thread.sleep(1000);							
						} else {				              
							Thread.sleep(500);							
						}
						continue;
					} else {
					/*
						if(!(myTask.getTask().isMapTask())||(myTask.getTask().isMapTask()&&myTask.getTask().getPartition() > 7 && myTask.getTask().getPartition() <30)) {						
							LOG.info("System.exit(0); " + myTask.getTask());
							//System.exit(0);
							continue;
						}*/
						if (runningTasks.containsKey(myTask.getTask().getTaskID())) {
							if (myTask.getTask().isTaskCleanupTask()) {
								killTask(myTask.getTask().getTaskID());
							}
						}
						synchronized (pendingTasks) {
							
							pendingTasks.add(myTask);
							pendingTasks.notifyAll();
							LOG.info("cm********** GetNewTask: " + myTask.getTask());
							idleLoopCount = 0;
						}
					}					
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block					
					this.isEnded = true;
					LOG.info("System.exit(31)");
					System.exit(31);
					continue;		
				} catch (IOException e) {
					// TODO Auto-generated catch block
					this.isEnded = true;
					LOG.info("System.exit(30)");
					System.exit(30);
					continue;					
				} 
			}while(!isEnded);
		}
	}
	
	private class LaunchTaskThread extends Thread{
		private boolean isEnded = false;
		void end() {
			this.interrupt();
			this.isEnded = true;
		}
		public void run() {			
			while (!isEnded) {
				synchronized (pendingTasks) {
					while (pendingTasks.size() == 0) {
						try {
							pendingTasks.wait();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							this.isEnded = true;
							continue;
						}
					}
					JvmTask myTask = pendingTasks.remove(0);				
					Task task = myTask.getTask();				
					task.setJvmContext(jvmContext);				
					RunningTask rt = new RunningTask(task);
					if (task.isTaskCleanupTask()) {
						cleanupTasks.put(task.getTaskID(), rt);
					} else {						
						runningTasks.put(task.getTaskID(), rt);						
					}
					
					if (rt.initialize()) {
						rt.setName(task.getTaskID()+"");		
						rt.start();
						LOG.info("cm********** LaunchNewTask: " + myTask.getTask());
					}
					
				}
			}
		}
	}
	
	private class RunningTask extends Thread{
		Task task;
		UserGroupInformation childUGI;
		JobConf job;
		TaskAttemptID taskid = null;
		boolean isCleanup;
		boolean isEnd = false;
		boolean currentJobSegmented = true;
		
		boolean isCleanup() {
			return task.isTaskCleanupTask();
		}
		boolean getCurrentJobSegmented() {
			return currentJobSegmented;
		}
		TaskAttemptID getTaskAttemptId() {
			return task.getTaskID();
		}
		Task getTask() {
			return this.task;
		}
		RunningTask(Task t) {
			this.task = t;
			//initialize();
		}
		boolean initialize() {			
			//URL[] 
			
			taskid = task.getTaskID();
			task.setJvmContext(jvmContext);        
			isCleanup = isCleanup();
			// Create the JobConf and determine if this job gets segmented task logs
			job = new JobConf(task.getJobFile());
			currentJobSegmented = logIsSegmented(job);

			// reset the statistics for the task
			//FileSystem.clearStatistics();
    
			// Set credentials
			job.setCredentials(defaultConf.getCredentials());
			//forcefully turn off caching for localfs. All cached FileSystems
			//are closed during the JVM shutdown. We do certain
			//localfs operations in the shutdown hook, and we don't
			//want the localfs to be "closed"
			job.setBoolean("fs.file.impl.disable.cache", false);

			// set the jobTokenFile into task
			task.setJobTokenSecret(JobTokenSecretManager.
					createSecretKey(jt.getPassword()));

			// setup the child's mapred-local-dir. The child is now sandboxed and
			// can only see files down and under attemtdir only.
			TaskRunner.setupChildMapredLocalDirs(task, job);
			try {
				// setup the child's attempt directories
				localizeTask(task, job, logLocation);

        //setupWorkDir actually sets up the symlinks for the distributed
        //cache. After a task exits we wipe the workdir clean, and hence
        //the symlinks have to be rebuilt.
        TaskRunner.setupWorkDir(job, new File(cwd));
        
        //create the index file so that the log files 
        //are viewable immediately
        TaskLog.syncLogs
          (logLocation, taskid, isCleanup, logIsSegmented(job));
        	
        task.setConf(job);

        if (firstConf == null) {
  				firstConf = job;
  				try {
  					jvmMemManager.setConf(firstConf);
  				} catch (IOException e) {
  					// TODO Auto-generated catch block
  					e.printStackTrace();
  				}
  			}
        List<String> classpaths = new ArrayList<String>();
        TaskRunner.appendJobJarClasspaths(job.getJar(), classpaths);
        final URL[] urls = new URL[classpaths.size()];
        for (int i = 0; i < classpaths.size(); ++i) {
          urls[i] = new File(classpaths.get(i)).toURI().toURL();
        }
  			this.setContextClassLoader(new URLClassLoader(urls, 			
  					getContextClassLoader()));
  			
        LOG.debug("Creating remote user to execute task: " + job.get("user.name"));
        childUGI = UserGroupInformation.createRemoteUser(job.get("user.name"));
        // Add tokens to new user so that it may execute its task correctly.
        for(Token<?> token : UserGroupInformation.getCurrentUser().getTokens()) {
          childUGI.addToken(token);
        } 
        
			} catch (Exception exception) {
				LOG.warn("Error running child", exception);
				try {
					if (task != null) {
						// do cleanup for the task
						if(childUGI == null) {
							task.taskCleanup(umbilical);
						} else {
							final Task taskFinal = task;
							childUGI.doAs(new PrivilegedExceptionAction<Object>() {
								@Override
								public Object run() throws Exception {
									taskFinal.taskCleanup(umbilical);
									return null;
								}
							});
						}
					}
				} catch (Exception e) {
					LOG.info("Error cleaning up", e);
				}
				// Report back any failures, for diagnostic purposes
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				exception.printStackTrace(new PrintStream(baos));
				if (this.getTaskAttemptId() != null) {
					try {
						umbilical.reportDiagnosticInfo(this.getTaskAttemptId(), baos.toString(), jvmContext);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				LOG.info("cm********** initializeTask failed : " + task);
				taskEnd();
				return false;
			}
			return true;
		}
		
		public void taskEnd() {
			LOG.info("cm********** task end : " + task);
			if (task.isTaskCleanupTask()) {
				cleanupTasks.remove(taskid);
			} else { 
				synchronized(runningTasks) {
					runningTasks.remove(taskid);
					runningTasks.notifyAll();
					isEnd = true;
				}
			}
		}
		
		public void run() {
			job = new JobConf(task.getJobFile());
			Thread t = new Thread() {
				public void run() {
					//every so often wake up and syncLogs so that we can track
					//logs of the currently running task
					while (!task.isKilled && !isEnd) {
						try {
							Thread.sleep(5000);
							if (taskid != null) {
								TaskLog.syncLogs
								(logLocation, taskid, isCleanup, currentJobSegmented);
							}
						} catch (InterruptedException ie) {
						} catch (IOException iee) {
							LOG.error("Error in syncLogs: " + iee);
							//System.exit(-1);
							return;
						}
					}
				}
			};
			t.setName("Thread for syncLogs");
			t.setDaemon(true);
			//t.start();
		    
			// Create a final reference to the task for the doAs block
			final Task taskFinal = task;
			try {
				if (childUGI == null) {
					return;
				}
				childUGI.doAs(new PrivilegedExceptionAction<Object>() {
					@Override
					public Object run() throws Exception {
						try {
							// use job-specified working directory
							FileSystem.get(job).setWorkingDirectory(job.getWorkingDirectory());
							if (taskFinal.isMapTask()) {
								LOG.info("taskFinal  ::::  " + taskFinal);
								if (jvmMemManager.getConf() == null) {
									jvmMemManager.setConf(job);
								}
								taskFinal.setJvmMemManager(jvmMemManager);
							}
							while (taskFinal.isTaskCleanupTask() && runningTasks.containsKey(taskFinal.getTaskID())) {
								synchronized(runningTasks) {
									runningTasks.wait(500);
								}
							}
							taskFinal.run(job, umbilical);        // run the task
							LOG.info("cm ******** task ran : " + taskFinal);
						} finally {
							if (task.isKilled) {
								return null;
							}
							TaskLog.syncLogs
							(logLocation, taskid, isCleanup, logIsSegmented(job));
							TaskLogsTruncater trunc = new TaskLogsTruncater(defaultConf);
							trunc.truncateLogs(new JVMInfo(
									TaskLog.getAttemptDir(taskFinal.getTaskID(),
											taskFinal.isTaskCleanupTask()), Arrays.asList(taskFinal)));
						}
							
						return null;
					}
				});
			} catch (FSError e) {
				LOG.fatal("FSError from child", e);
				try {
					umbilical.fsError(taskid, e.getMessage(), jvmContext);
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			} catch (Exception exception) {
				LOG.warn("Error running child", exception);
				try {
					if (task != null) {
						// do cleanup for the task
						if(childUGI == null) {
							task.taskCleanup(umbilical);
						} else {	                  
							childUGI.doAs(new PrivilegedExceptionAction<Object>() {
								@Override
								public Object run() throws Exception {
									task.taskCleanup(umbilical);
									return null;
								}
							});
						}
					}
				} catch (Exception e) {
					LOG.info("Error cleaning up", e);
				}
				// Report back any failures, for diagnostic purposes
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				exception.printStackTrace(new PrintStream(baos));
				if (taskid != null) {
					try {
						umbilical.reportDiagnosticInfo(taskid, baos.toString(), jvmContext);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			} catch (Throwable throwable) {
				LOG.fatal("Error running child : "
						+ StringUtils.stringifyException(throwable));
				if (taskid != null) {
					Throwable tCause = throwable.getCause();
					String cause = tCause == null 
							? throwable.getMessage() 
									: StringUtils.stringifyException(tCause);
							try {
								umbilical.fatalError(taskid, cause, jvmContext);
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
				}
			}
			finally {				
				taskEnd();
				t.interrupt();
				return;
			}
		}
		
	}
	
	
  public static final Log LOG =
    LogFactory.getLog(Child.class);

   
  static String cwd;

  static boolean logIsSegmented(JobConf job) {
    return (job.getNumTasksToExecutePerJvm() != 1);
  }

  public void start() {
  	this.launchTaskThread.start();
  	this.getNewTaskThread.start();
  }
  public void stop() {
  	this.launchTaskThread.end();
  	this.getNewTaskThread.end();
  }
  public static void main(String[] args) throws Throwable {
    LOG.debug("Child starting");
    
    String host = args[0];
    int port = Integer.parseInt(args[1]);
    final InetSocketAddress address = NetUtils.makeSocketAddr(host, port);
    final TaskAttemptID firstTaskid = TaskAttemptID.forName(args[2]);
    logLocation = args[3];
    //final int SLEEP_LONGER_COUNT = 5;
    int jvmIdInt = Integer.parseInt(args[4]);
    JVMId jvmId = new JVMId(firstTaskid.getJobID(),jvmIdInt);
    String prefix = firstTaskid.isMap() ? "MapTask" : "ReduceTask";
    
    cwd = System.getenv().get(TaskRunner.HADOOP_WORK_DIR);
    if (cwd == null) {
      throw new IOException("Environment variable " + 
                             TaskRunner.HADOOP_WORK_DIR + " is not set");
    }

    // file name is passed thru env
    String jobTokenFile = 
      System.getenv().get(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
    Credentials credentials = 
      TokenCache.loadTokens(jobTokenFile, defaultConf);
    
    LOG.debug("loading token. # keys =" +credentials.numberOfSecretKeys() + 
        "; from file=" + jobTokenFile);
    
    jt = TokenCache.getJobToken(credentials);
    SecurityUtil.setTokenService(jt, address);
    UserGroupInformation current = UserGroupInformation.getCurrentUser();
    current.addToken(jt);

    UserGroupInformation taskOwner 
     = UserGroupInformation.createRemoteUser(firstTaskid.getJobID().toString());
    taskOwner.addToken(jt);
    
    // Set the credentials
    defaultConf.setCredentials(credentials);
    
    umbilical = 
      taskOwner.doAs(new PrivilegedExceptionAction<TaskUmbilicalProtocol>() {
        @Override
        public TaskUmbilicalProtocol run() throws Exception {
          return (TaskUmbilicalProtocol)RPC.getProxy(TaskUmbilicalProtocol.class,
              TaskUmbilicalProtocol.versionID,
              address,
              defaultConf);
        }
    });
   // tasktracker = umbilical;
    
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        try {
          if (runningTasks.size() != 0) {
        	  for (RunningTask t : runningTasks.values()) {
        		  TaskLog.syncLogs
        		  (logLocation, t.getTaskAttemptId(), t.isCleanup(), t.getCurrentJobSegmented());
        	  }
          }
        } catch (Throwable throwable) {
        }
      }
    });
    
    
    String pid = "";
    if (!Shell.WINDOWS) {
      pid = System.getenv().get("JVM_PID");
    }
    JvmContext context = new JvmContext(jvmId, pid);
    int idleLoopCount = 0;
    Task task = null;
    
    UserGroupInformation childUGI = null;
   // Child.jvmContext = context;
    jvmContext = context;
    //这样设计只能统计一个进程的数据读写数据。
    FileSystem.clearStatistics();
    
 // Initiate Java VM metrics
    initMetrics(prefix, jvmId.toString(), defaultConf.getSessionId());
    
    Child child = new Child();
   // jvmMemManager.setConf(defaultConf);
    child.start();
    
    //wait for finish
    synchronized (finishSignal) {
    	finishSignal.wait();
    }
    LOG.info("finishSignal waited");
    child.stop();
    
    LOG.info(jvmId.toString() + "finished" );
    
	  RPC.stopProxy(umbilical);
	  shutdownMetrics();
	  // Shutting down log4j of the child-vm... 
	  // This assumes that on return from Task.run() 
	  // there is no more logging done.  
	  LogManager.shutdown();	  
	  LOG.info("child System.exit(0)");
	  System.exit(0);    
  }

  private static void initMetrics(String prefix, String procName,
                                  String sessionId) {
    DefaultMetricsSystem.initialize(prefix);  
    JvmMetricsSource.create(procName, sessionId);
  }

  private static void shutdownMetrics() {
    DefaultMetricsSystem.INSTANCE.shutdown();
  }

  public static Task getTask(TaskAttemptID tid) {  	
  	if (runningTasks.get(tid) == null) {
  		return null;
  	} else {
  		return runningTasks.get(tid).getTask();
  	}
  }
  static void localizeTask(Task task, JobConf jobConf, String logLocation) 
  throws IOException{
    
    // Do the task-type specific localization
    task.localizeConfiguration(jobConf);
    
    //write the localized task jobconf
    LocalDirAllocator lDirAlloc = 
      new LocalDirAllocator(JobConf.MAPRED_LOCAL_DIR_PROPERTY);
    Path localTaskFile =
      lDirAlloc.getLocalPathForWrite(TaskTracker.JOBFILE, jobConf);
    JobLocalizer.writeLocalJobFile(localTaskFile, jobConf);
    task.setJobFile(localTaskFile.toString());
    task.setConf(jobConf);
  }
  
  public static void killTask(TaskAttemptID tid) {
  	LOG.info("kill Task : " + tid);
  	if (!runningTasks.containsKey(tid)) {
  		return;
  	} else {
  		if (runningTasks.get(tid).getTask().isKilled) {
  			return;
  		}
  		runningTasks.get(tid).getTask().kill();
  		if (tid.isMap()) {
  			jvmMemManager.kill(tid);
  		}
  		try {
				runningTasks.get(tid).join();
				if (cleanupTasks.containsKey(tid)) {
					RunningTask rt = cleanupTasks.get(tid);
					synchronized (rt) {
						rt.notify();
					}
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block				
				e.printStackTrace();
			}  		
  		if (runningTasks.get(tid) != null) {
  			runningTasks.get(tid).taskEnd();
  		}
  		LOG.info("Task " + tid + " is killed");
  	}  	
  }
}
