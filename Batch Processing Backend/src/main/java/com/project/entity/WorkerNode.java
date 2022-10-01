package com.project.entity;

import java.util.Date;

import org.springframework.batch.core.BatchStatus;

import lombok.Data;

@Data
public class WorkerNode {
	private String stepName;
	private BatchStatus status;
	private Date startTime;
	private Date endTime;
	private int readCount;
	private int writeCount;
}

