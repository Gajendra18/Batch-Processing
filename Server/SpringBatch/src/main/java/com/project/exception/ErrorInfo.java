package com.project.exception;

import java.time.LocalDateTime;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ErrorInfo {
	private String errorMessage;
	private Integer errorCode;
	private LocalDateTime timeStamp;
}
