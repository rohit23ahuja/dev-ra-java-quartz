package dev.ra.java.quartz;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class VerySimpleJobListener implements JobListener {

	private static Logger _log = LoggerFactory.getLogger(VerySimpleJobListener.class);
    public static final String FAVORITE_COLOR = "favorite color";

	public String getName() {
		return "VerySimpleJobListener";
	}

	public void jobToBeExecuted(JobExecutionContext inContext) {
		_log.info("VerySimpleJobListener|Job Execution to start soon.");
	}

	public void jobExecutionVetoed(JobExecutionContext inContext) {
		_log.info("VerySimpleJobListener|Vetoed.");
	}

	public void jobWasExecuted(JobExecutionContext inContext, JobExecutionException inException) {
		_log.info("VerySimpleJobListener|Job Executed.");
	}
}
