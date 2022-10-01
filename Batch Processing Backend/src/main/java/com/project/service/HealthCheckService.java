package com.project.service;

import org.quartz.SchedulerException;

import java.time.LocalDateTime;

public interface HealthCheckService {

    public void doHealthCheck(Long jobExecutionId, String partitionName, LocalDateTime nextUpdate) throws SchedulerException;
}
