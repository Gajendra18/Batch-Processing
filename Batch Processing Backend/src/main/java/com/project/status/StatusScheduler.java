package com.project.status;

import com.project.entity.WorkerNode;
import com.project.entity.WorkerStatus;
import com.project.exception.BatchException;
import com.project.service.BatchService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@Configuration
@EnableScheduling
@Profile("worker")
@Slf4j
public class StatusScheduler {

    @Autowired
    private BatchService batchService;

    @Autowired
    private KafkaTemplate<String, WorkerStatus> kafkaTemplate;

    @Value("${spring.cloud.task.name:default}")
    private String taskName;

    @Value("${spring.cloud.task.job-execution-id:0}")
    private String jobId;


    private static final String TOPIC = "WorkerStatus";


    @Scheduled(fixedRate = 5000)
    void sendStatus() throws BatchException {
        String[] tasknameArray = taskName.split(":");
        WorkerStatus status = new WorkerStatus();
        status.setJobExecutionId(Integer.parseInt(jobId));
        WorkerNode workernode = batchService.getWorkerNode(Long.valueOf(jobId), tasknameArray[tasknameArray.length - 1]);
        status.setStatus("running");
        status.setWorkernode(workernode);
        kafkaTemplate.send(TOPIC, status);
    }


}
