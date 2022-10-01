package com.project.utility;


import com.project.constants.Constants;
import com.project.entity.JobScheduleDetails;
import com.project.exception.BatchException;
import lombok.extern.slf4j.Slf4j;
import org.quartz.*;
import org.quartz.impl.matchers.GroupMatcher;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobInstanceException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
@Slf4j
@Component
public class validatorService {

    @Autowired
    MongoTemplate mongoTemplate;

    @Autowired
    private JobExplorer explorer;

    @Autowired
    private Job job;

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    private Scheduler scheduler;


    public void checkJobName(String jobName) throws BatchException {
        List<JobInstance> instanceIds = explorer.getJobInstances(job.getName(), 0, Integer.MAX_VALUE);
        for (JobInstance id : instanceIds) {
            Long executionId;
            try {
                executionId = jobOperator.getExecutions(id.getInstanceId()).get(0);
            } catch (NoSuchJobInstanceException e) {
                throw new BatchException(e.getMessage());
            }
            Optional<JobExecution> optionalJobExecution = Optional.ofNullable(explorer.getJobExecution(executionId));
            JobExecution jobExecution = optionalJobExecution
                    .orElseThrow(() -> new BatchException("Service.JOB_EXECUTION_NOT_FOUND"));
            log.info(jobExecution.getJobParameters().getString(Constants.JOB_NAME));
            if (jobExecution.getJobParameters().getString(Constants.JOB_NAME).equalsIgnoreCase(jobName)) {
                throw new BatchException("Service.JOB_ALREADY_EXISTS");
            }
        }

        try {
            for (String groupName : scheduler.getJobGroupNames()) {
                try {
                    for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.jobGroupEquals(groupName))) {
                        String job = jobKey.getName().split(":")[0];
                        log.info(job);
                        if (job.equalsIgnoreCase(jobName)){
                            throw new BatchException("Service.JOB_ALREADY_EXISTS");
                        };
                    }
                } catch (SchedulerException e) {
                    throw new BatchException(e.getMessage());
                }
            }
        } catch (SchedulerException e) {
            throw new BatchException(e.getMessage());
        }
    }

    public void checkCollectionName(String collectionName) throws BatchException {
        if (mongoTemplate.getCollectionNames().contains(collectionName)) {
            throw new BatchException("Service.TABLE_ALREADY_EXISTS");
        }
        mongoTemplate.createCollection(collectionName);
    }
}
