package com.project.quartzjobconfig;

import java.util.HashMap;
import java.util.Map;

import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.web.context.support.SpringBeanAutowiringSupport;

import com.project.constants.Constants;

public class QuartzJob extends QuartzJobBean{
	
	@Autowired
	private JobLauncher jobLauncher;
	
	@Autowired
	private Job job;

	@Override
	protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
		SpringBeanAutowiringSupport.processInjectionBasedOnCurrentContext(this);
		JobDataMap datamap = context.getJobDetail().getJobDataMap();
		
		Map<String, JobParameter> maps = new HashMap<>();
		maps.put(Constants.TIME, new JobParameter(System.currentTimeMillis()));
		maps.put(Constants.INPUT_FILES, new JobParameter(datamap.getString(Constants.INPUT_FILES)));
		maps.put(Constants.PARTITION_SIZE,new JobParameter(Integer.toString(datamap.getInt(Constants.PARTITION_SIZE))));
		maps.put(Constants.MAIL_RECIPIENTS, new JobParameter(datamap.getString(Constants.MAIL_RECIPIENTS)));
		maps.put(Constants.JOB_NAME, new JobParameter(datamap.getString(Constants.JOB_NAME)));
		maps.put(Constants.JOB_DESCRIPTION, new JobParameter(datamap.getString(Constants.JOB_DESCRIPTION)));
		maps.put(Constants.COLLECTION_NAME, new JobParameter(datamap.getString(Constants.COLLECTION_NAME)));
		maps.put(Constants.JOB_RESTART, new JobParameter(Boolean.toString(datamap.getBoolean(Constants.JOB_RESTART))));
		maps.put("WORKERS", new JobParameter(Long.toString(datamap.getLong("WORKERS"))));
		JobParameters jobParameters = new JobParameters(maps);
		try {
			jobLauncher.run(job, jobParameters);
		} catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException
				| JobParametersInvalidException e) {
			e.printStackTrace();
		}
		
		
	}
	
}
