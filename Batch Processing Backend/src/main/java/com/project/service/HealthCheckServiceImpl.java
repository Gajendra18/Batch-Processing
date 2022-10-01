package com.project.service;

import com.project.quartzjobconfig.HealthCheckJob;
import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class HealthCheckServiceImpl implements HealthCheckService {

    @Autowired
    private Scheduler scheduler;

    @Override
    public void doHealthCheck(Long jobExecutionId, String partitionName, LocalDateTime nextUpdate) throws SchedulerException {
        JobDetail details = buildHealthCheckDetail(jobExecutionId + "_" + partitionName + "_");
        details.getJobDataMap().put("jobExecutionId", jobExecutionId);
        details.getJobDataMap().put("partitionName", partitionName);

        String cronExpression = nextUpdate.getSecond() + " " + nextUpdate.getMinute() + " " + nextUpdate.getHour() + " "
                + nextUpdate.getDayOfMonth() + " " + nextUpdate.getMonthValue() + " ? " + nextUpdate.getYear();

        Trigger trigger = buildHealthCheckTrigger(details, cronExpression);

        scheduler.scheduleJob(details, trigger);

    }

    private JobDetail buildHealthCheckDetail(String jobName) {
        return JobBuilder.newJob(HealthCheckJob.class).withIdentity(jobName + ":" + UUID.randomUUID().toString())
                .storeDurably().build();
    }

    private Trigger buildHealthCheckTrigger(JobDetail jobDetail, String cronExpression) {
        return TriggerBuilder.newTrigger().forJob(jobDetail).withIdentity(jobDetail.getKey().getName())
                .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression)).build();
    }
}
