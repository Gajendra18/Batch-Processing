package com.project.quartzjobconfig;

import com.project.constants.HealthConstants;
import com.project.entity.WorkerInfo;
import com.project.exception.BatchException;
import com.project.repository.WorkerRepository;
import com.project.service.BatchService;
import com.project.service.HealthCheckService;
import lombok.extern.slf4j.Slf4j;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerException;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.web.context.support.SpringBeanAutowiringSupport;

import java.time.LocalDateTime;
@Slf4j
public class HealthCheckJob extends QuartzJobBean{

	@Autowired
	private HealthCheckService healthCheckService;

	@Autowired
	private BatchService batchService;


	@Autowired
	private WorkerRepository workerRepository;

	@Override
	protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
		SpringBeanAutowiringSupport.processInjectionBasedOnCurrentContext(this);
		JobDataMap dataMap = context.getJobDetail().getJobDataMap();
		log.info("Health check");
		long jobId = dataMap.getLong("jobExecutionId");
		String partition = dataMap.getString("partitionName");
		WorkerInfo log = workerRepository.findStatus(jobId, partition);
		if (log.getStatus().equals(HealthConstants.INITIALIZED)) {
			log.setRetryCount(log.getRetryCount() + 1);
			workerRepository.save(log);
			if (log.getRetryCount() <= 2) {
				try {
					healthCheckService.doHealthCheck(jobId, partition, LocalDateTime.now().plusSeconds(10));
				} catch (SchedulerException e) {
					throw new RuntimeException(e);
				}
			} else {
				try {
					batchService.stopJobWithExecutionId(jobId);
				} catch (BatchException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}
	
}
