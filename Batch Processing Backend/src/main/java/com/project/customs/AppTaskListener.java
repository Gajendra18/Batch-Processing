package com.project.customs;

import com.project.constants.HealthConstants;
import com.project.entity.WorkerInfo;
import com.project.entity.WorkerNode;
import com.project.entity.WorkerStatus;
import com.project.repository.WorkerRepository;
import com.project.service.BatchService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.task.listener.annotation.AfterTask;
import org.springframework.cloud.task.listener.annotation.BeforeTask;
import org.springframework.cloud.task.repository.TaskExecution;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Slf4j
@Transactional
public class AppTaskListener {
    @Autowired
    private WorkerRepository workerRepository;

    @Autowired
    private BatchService batchService;

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private KafkaTemplate<String, WorkerStatus> kafkaTemplate;

    private static final String TOPIC = "WorkerStatus";


    @BeforeTask
    public void beforeTaskInvocation(TaskExecution taskExecution) {
        log.info("Before task");
        String[] partitionName = taskExecution.getTaskName().split("_");
        WorkerInfo workerInfo = workerRepository.findByTaskExecutionAndPartition(taskExecution.getExecutionId(), partitionName[partitionName.length - 1]);
        try {
            workerInfo.setStatus(HealthConstants.RUNNING);
            workerInfo.setLastUpdated(LocalDateTime.now());

            WorkerStatus status = new WorkerStatus();
            status.setJobExecutionId((int) workerInfo.getJobExecutionId());
            status.setStatus("stepStarted");
            WorkerNode workernode;
            try {
                workernode = batchService.getWorkerNode(workerInfo.getJobExecutionId(), partitionName[partitionName.length - 1]);
                status.setWorkernode(workernode);
            } catch (Exception e) {
                e.printStackTrace();
            }
            kafkaTemplate.send(TOPIC, status);

        } catch (Exception e) {
            log.info("record not found");
        }
    }

    @AfterTask
    public void afterTaskInvocation(TaskExecution taskExecution) {
        log.info("After task");
        String[] partitionName = taskExecution.getTaskName().split("_");
        WorkerInfo workerInfo = workerRepository.findByTaskExecutionAndPartition(taskExecution.getExecutionId(), partitionName[partitionName.length - 1]);
        try {
            workerInfo.setStatus(HealthConstants.COMPLETED);
            workerInfo.setLastUpdated(LocalDateTime.now());

            WorkerStatus status = new WorkerStatus();
            status.setJobExecutionId((int) workerInfo.getJobExecutionId());
            status.setStatus("stepCompleted");
            WorkerNode workernode;
            try {
                workernode = batchService.getWorkerNode(workerInfo.getJobExecutionId(), partitionName[partitionName.length - 1]);
                status.setWorkernode(workernode);
            } catch (Exception e) {
                e.printStackTrace();
            }
            kafkaTemplate.send(TOPIC, status);


        } catch (Exception e) {
            log.info("record not found");
        }
    }
}
