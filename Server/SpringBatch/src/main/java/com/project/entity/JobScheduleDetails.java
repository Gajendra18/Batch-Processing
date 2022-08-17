package com.project.entity;

public class JobScheduleDetails extends JobParams{
	
	private String jobGroup;
	private String cronExpression;
	
	
	public String getJobGroup() {
		return jobGroup;
	}
	
	public void setJobGroup(String jobGroup) {
		this.jobGroup = jobGroup;
	}
	
	public String getCronExpression() {
		return cronExpression;
	}
	
	public void setCronExpression(String cronExpression) {
		this.cronExpression = cronExpression;
	}
	
	
}
