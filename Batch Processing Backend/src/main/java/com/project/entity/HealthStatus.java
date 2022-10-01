package com.project.entity;

import java.util.Date;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class HealthStatus {
	private Long jobExecutionId;
	private String workerName;
	private String status;
	private Date lastUpdated;
	private int count;
}
