package com.project.entity;

import lombok.Data;

@Data
public class WorkerStatus {
	private Long jobInstanceId;
	private int jobExecutionId;
	private String status;
	private WorkerNode workernode;
}
