
package dev.ra.java.quartz;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

import java.util.Date;

import org.quartz.DateBuilder;
import org.quartz.DateBuilder.IntervalUnit;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobListener;
import org.quartz.Matcher;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.KeyMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JavaQuartzExample {

	private static Logger _log = LoggerFactory.getLogger(JavaQuartzExample.class);

	public void run() throws Exception {

		_log.info("Scheduler|Initialization");

		SchedulerFactory schedulerFactory = new StdSchedulerFactory();
		Scheduler scheduler = schedulerFactory.getScheduler();

		// Delete all exisiting jobs
		//sched.clear();

		_log.info("Scheduler|Initialization|Complete");
		JobDetail jobDetailExisting = scheduler.getJobDetail(JobKey.jobKey("VerySimpleJob", "VerySimpleJobGroup"));
		if (jobDetailExisting == null) {
			// 2 minutes from now
			Date runTime = DateBuilder.futureDate(2, IntervalUnit.MINUTE);

			_log.info("Job|Scheduling");
			JobDetail jobDetail = newJob(VerySimpleJob.class).withIdentity("VerySimpleJob", "VerySimpleJobGroup")
					.usingJobData(VerySimpleJob.FAVORITE_COLOR, "Black").usingJobData(VerySimpleJob.EXECUTION_COUNT, 1)
					.storeDurably(false).build();

			Trigger trigger = newTrigger().withIdentity("VerySimpleJobTrigger", "VerySimpleJobGroup").startAt(runTime).build();

			_log.info("Scheduler|Registering Listener");
			JobListener listener = new VerySimpleJobListener();
			Matcher<JobKey> matcher = KeyMatcher.keyEquals(jobDetail.getKey());
			scheduler.getListenerManager().addJobListener(listener, matcher);

			Date scheduledTime = scheduler.scheduleJob(jobDetail, trigger);
			_log.info("Job|Scheduled| Key {} Time {}", jobDetail.getKey(), scheduledTime);
		}

		scheduler.start();
		_log.info("Scheduler|Started");

		_log.info("Scheduled will shutdown after 10 minutes");
		try {
			Thread.sleep(600L * 1000L);
		} catch (InterruptedException e) {
			_log.error("Thread Interupted", e);
		}

		scheduler.shutdown(true);
		_log.info("Scheduler|Shutdown Complete");
	}

	public static void main(String[] args) throws Exception {
		JavaQuartzExample example = new JavaQuartzExample();
		example.run();
	}
}
