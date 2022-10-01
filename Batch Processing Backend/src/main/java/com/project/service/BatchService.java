package com.project.service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.springframework.web.multipart.MultipartFile;

import com.amazonaws.services.s3.model.S3Object;
import com.project.entity.BatchJob;
import com.project.entity.JobScheduleDetails;
import com.project.entity.WorkerNode;
import com.project.exception.BatchException;

public interface BatchService {
	public Long runJob(List<MultipartFile> multipartFile, String params) throws BatchException;

	public Long restartJob(Long jobId) throws BatchException;

	public Boolean stopJob(Long id) throws BatchException;

	public void stopJobWithExecutionId(Long jobExecutionId) throws BatchException;

	public List<BatchJob> getAllJobs() throws BatchException;

	public void startupClean() throws BatchException;

	public List<BatchJob> getAllJobExecutions(Long jonInstanceId) throws BatchException;

	public List<WorkerNode> getAllWorkerNodes(Long jobExecutionId) throws BatchException;

	public List<String> getWorkerLogs(Long jobExecutionID, String partitionName) throws BatchException;

	public Boolean scheduleJob(List<MultipartFile> multipartFile, String params) throws BatchException;

	public List<JobScheduleDetails> getScheduledJobs() throws BatchException;

	public Boolean unscheduleJob(String jobName, String jobGroup) throws BatchException;

	public WorkerNode getWorkerNode(Long jobExecutionId, String partitionName) throws BatchException;
	
	public String uploadFile(MultipartFile file);

	public byte[] downloadCsv(Long jobId) throws BatchException, IOException;

	public void updateInCompleteWorkers();

	public void updateWorkers(long jobExecutionId, String taskName, long taskExecutionId, String logName);

	public Map<String, String> getCollectionSchema(Long jobId) throws BatchException;
}
