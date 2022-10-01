package com.project.exception;

import java.time.LocalDateTime;

import org.apache.poi.EmptyFileException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.multipart.MaxUploadSizeExceededException;

@RestControllerAdvice
public class ExceptionControllerAdvice {

	@ExceptionHandler(Exception.class)
	public ResponseEntity<ErrorInfo> exceptionHandler(Exception exception) {
		ErrorInfo errorInfo = ErrorInfo.builder().errorMessage(exception.toString())
				.errorCode(HttpStatus.INTERNAL_SERVER_ERROR.value())
				.timeStamp(LocalDateTime.now())
				.build();
		return new ResponseEntity<>(errorInfo, HttpStatus.INTERNAL_SERVER_ERROR);
	}

	@ExceptionHandler(MaxUploadSizeExceededException.class)
	public ResponseEntity<ErrorInfo> maximumUploadSizeExceptionHandler(Exception exception) {
		ErrorInfo errorInfo = ErrorInfo.builder().errorMessage("Service.MAX_UPLOAD_SIZE_EXCEEDED")
				.errorCode(HttpStatus.BAD_REQUEST.value())
				.timeStamp(LocalDateTime.now())
				.build();
		return new ResponseEntity<>(errorInfo, HttpStatus.BAD_REQUEST);
	}

	@ExceptionHandler(EmptyFileException.class)
	public ResponseEntity<ErrorInfo> emptyFileExceptionHandler(Exception exception) {
		ErrorInfo errorInfo = ErrorInfo.builder().errorMessage("Service.EMPTY_FILE")
				.errorCode(HttpStatus.BAD_REQUEST.value())
				.timeStamp(LocalDateTime.now())
				.build();
		return new ResponseEntity<>(errorInfo, HttpStatus.BAD_REQUEST);
	}





	@ExceptionHandler(BatchException.class)
	public ResponseEntity<ErrorInfo> batchExceptionHandler(BatchException exception) {
		ErrorInfo errorInfo = ErrorInfo.builder().errorMessage(exception.getMessage())
				.errorCode(HttpStatus.BAD_REQUEST.value())
				.timeStamp(LocalDateTime.now())
				.build();
		return new ResponseEntity<>(errorInfo, HttpStatus.BAD_REQUEST);
	}
}
