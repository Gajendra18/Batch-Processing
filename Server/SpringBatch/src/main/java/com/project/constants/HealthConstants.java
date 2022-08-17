package com.project.constants;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HealthConstants {
	public static final String INITIALIZED = "INITIALIZED";
	public static final String RUNNING = "RUNNING";
	public static final String COMPLETED = "COMPLETED";
	public static final String FAILED = "FAILED";
}
