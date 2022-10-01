package com.project.jobConfiguration;

import com.project.repository.WorkerRepository;
import com.project.jobConfiguration.SimpleJobLauncher;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Component
@EnableScheduling
@Profile("!worker")
@Slf4j
public class JobQueueScheduler {


    KafkaConsumer<String, Long> consumer;

    @Autowired
    private WorkerRepository workerRepository;

    @Autowired
    private JobExplorer explorer;

    @Autowired
    private Job job;

    @Autowired
    private SimpleJobLauncher jobLauncher;

    @Value("${batch.job.max.workers}")
    private int maxWorkers;

    @PostConstruct
    public void JobQueueTemplate() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "JobEvent");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        this.consumer = new KafkaConsumer<>(config);
        this.consumer.subscribe(Collections.singletonList("JobEventQueue"));

    }


    @Scheduled(fixedRate = 20000)
    void jobConsumer() throws JobExecutionAlreadyRunningException, JobRestartException {
        log.info("Active Workers : " + workerRepository.findActiveWorkers());
        if (workerRepository.findActiveWorkers() < maxWorkers) {
            ConsumerRecords<String, Long> records = consumer.poll(1000);
            this.consumer.commitSync();
            log.info("record count : " + records.count());
            if (!records.isEmpty()) {
                for (ConsumerRecord<String, Long> record : records) {
                    Long jobExecutionId = record.value();
                    Optional<JobExecution> optionalJobExecution = Optional.ofNullable(explorer.getJobExecution(jobExecutionId));
                    JobExecution jobExecution = optionalJobExecution.orElse(null);
                    if ( jobExecution != null && !( jobExecution.getStatus().equals(BatchStatus.STOPPING) || jobExecution.getStatus().equals(BatchStatus.STOPPED)) ) {
                        jobLauncher.runJob(job, optionalJobExecution.get());
                    }
                }
            }
        }
    }


}
