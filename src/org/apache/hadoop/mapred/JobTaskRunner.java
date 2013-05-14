package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.mapred.TaskTracker.TaskInProgress;

public class JobTaskRunner extends TaskRunner {
	
	public JobTaskRunner(TaskInProgress task, TaskTracker tracker, JobConf conf,
			TaskTracker.RunningJob rjob) throws IOException {
		super(task, tracker, conf, rjob);	
	}

	/** Delete any temporary files from previous failed attempts. */
	public boolean prepare() throws IOException {	
		if (!super.prepare()) {
			return false;
		}
	
		mapOutputFile.removeAll();
		return true;
	}
	
	/** Delete all of the temporary map output files. */
	public void close() throws IOException {
		LOG.info(getTask()+" done; removing files.");
		if (!this.getTask().isMapOrReduce()) {
			mapOutputFile.removeAll();
		}
	}
	
	@Override
	public String getChildJavaOpts(JobConf jobConf, String defaultValue) {
		String user = 
				jobConf.get(JobConf.MAPRED_JOB_TASK_JAVA_OPTS, 
						super.getChildJavaOpts(jobConf, 
								JobConf.DEFAULT_MAPRED_TASK_JAVA_OPTS));
		String admin = 
				jobConf.get(TaskRunner.MAPRED_JOB_ADMIN_JAVA_OPTS,
						TaskRunner.DEFAULT_MAPRED_ADMIN_JAVA_OPTS);
		return user + " " + admin;
	}
	
	@Override
	public int getChildUlimit(JobConf jobConf) {
		return jobConf.getInt(JobConf.MAPRED_JOB_TASK_ULIMIT, 
				super.getChildUlimit(jobConf));
	}
	
	@Override
	public String getChildEnv(JobConf jobConf) {
		return jobConf.get(JobConf.MAPRED_JOB_TASK_ENV, super.getChildEnv(jobConf));
	}

}
