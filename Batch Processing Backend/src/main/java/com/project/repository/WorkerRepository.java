package com.project.repository;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;

import com.project.entity.WorkerInfo;

import java.util.List;

public interface WorkerRepository extends CrudRepository<WorkerInfo, Long>{

	@Query("SELECT c.externalExecutionId FROM WorkerInfo c WHERE c.jobExecutionId = :jobExecutionId AND c.taskName = :taskName")
	String findExternalExecutionId(@Param("jobExecutionId") Long jobExecutionId, @Param("taskName") String taskName);

	@Query("SELECT c FROM WorkerInfo c WHERE c.jobExecutionId = :jobExecutionId AND c.taskName = :taskName")
	WorkerInfo findStatus(@Param("jobExecutionId") Long jobExecutionId, @Param("taskName") String taskName);


	@Query("SELECT c FROM WorkerInfo c WHERE c.taskExecutionId = :taskExecutionId AND c.taskName = :taskName")
	WorkerInfo findByTaskExecutionAndPartition(@Param("taskExecutionId") Long taskExecutionId, @Param("taskName") String taskName);

	@Query("SELECT count(c) FROM WorkerInfo c WHERE c.status IN ('INITIALIZED', 'RUNNING', 'WAITING')")
	int findActiveWorkers();

	@Query("SELECT c FROM WorkerInfo c WHERE c.status IN ('INITIALIZED', 'RUNNING', 'WAITING')")
	List<WorkerInfo> findIncompleteWorkers();

	@Query("SELECT c FROM WorkerInfo c WHERE c.jobExecutionId = :jobExecutionId AND c.status IN ('INITIALIZED', 'RUNNING', 'WAITING')")
	List<WorkerInfo> findIncompleteWorkersForId(@Param("jobExecutionId") Long jobExecutionId);

	
}
