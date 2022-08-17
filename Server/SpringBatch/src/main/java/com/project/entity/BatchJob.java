package com.project.entity;

import java.util.Date;

import org.springframework.batch.core.BatchStatus;

import lombok.Data;

@Data
public class BatchJob {
	private Long jobID;
	private String jobName;
	private BatchStatus status;
	private Date createTime;
	private Date endTime;
	private JobParams jobParams;
	
}
