package dev.ra.java.quartz;

import java.util.Date;

import org.quartz.DateBuilder;
import org.quartz.DateBuilder.IntervalUnit;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.PersistJobDataAfterExecution;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class VerySimpleJob implements Job {

	private static Logger _log = LoggerFactory.getLogger(VerySimpleJob.class);
	public static final String FAVORITE_COLOR = "favorite color";
	public static final String EXECUTION_COUNT = "count";
	public static final String ERROR = "error";
	// Since Quartz will re-instantiate a class every time it
	// gets executed, members non-static member variables can
	// not be used to maintain state!
	private int _counter = 1;

	// Needed by quartz
	public VerySimpleJob() {
	}

	// Called by scheduler when a trigger is fired
	public void execute(JobExecutionContext context) throws JobExecutionException {

		JobKey jobKey = context.getJobDetail().getKey();
		JobDataMap data = context.getJobDetail().getJobDataMap();
		int count = data.getInt(EXECUTION_COUNT);
		String favoriteColor = data.getString(FAVORITE_COLOR);
		Scheduler scheduler = context.getScheduler();

		try {
			if (!favoriteColor.equals("Black")) {
				_log.info("VerySimpleJob: " + jobKey + " executing at " + new Date() + " favorite color is "
						+ favoriteColor + " execution count (from job map) is " + count 
						+ "  execution count (from job member variable) is " + _counter);

				count++;
				data.put(EXECUTION_COUNT, count);
				_counter++;
			} else {
				_log.info("VerySimpleJob|Rescheduling Job.");
				data.put(VerySimpleJob.FAVORITE_COLOR, "Green");
				data.put(VerySimpleJob.EXECUTION_COUNT, 1);

				Trigger existingTrigger = scheduler.getTrigger(TriggerKey.triggerKey("VerySimpleJobTrigger", "VerySimpleJobGroup"));
				TriggerBuilder<? extends Trigger> triggerBuilder = existingTrigger.getTriggerBuilder();
		        Trigger newTrigger = triggerBuilder.startAt(DateBuilder.futureDate(15, IntervalUnit.SECOND)).build();
		        scheduler.rescheduleJob(TriggerKey.triggerKey("VerySimpleJobTrigger", "VerySimpleJobGroup"),newTrigger);
			}
		} catch (Exception e) {
			_log.info("VerySimpleJob|Error", e);
			JobExecutionException e2 = new JobExecutionException(e);
			throw e2;
		}
	}

}
