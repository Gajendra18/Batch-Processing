package com.project.batchconfiguration;

import com.project.customs.*;
import com.project.utility.CsvParser;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.deployer.resource.support.DelegatingResourceLoader;
import org.springframework.cloud.deployer.spi.task.TaskLauncher;
import org.springframework.cloud.task.batch.partition.DeployerPartitionHandler;
import org.springframework.cloud.task.batch.partition.DeployerStepExecutionHandler;
import org.springframework.cloud.task.batch.partition.PassThroughCommandLineArgsProvider;
import org.springframework.cloud.task.batch.partition.SimpleEnvironmentVariablesProvider;
import org.springframework.cloud.task.configuration.EnableTask;
import org.springframework.cloud.task.repository.TaskRepository;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.IOException;
import java.util.*;

@Configuration
@EnableBatchProcessing
@EnableTask
@Slf4j
public class BatchConfig {

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Value("${jarLocation}")
    private String jarLocation;

    @Value("${batch.job.jobname}")
    private String jobName;

    @Value("${batch.job.max.workers}")
    private int maxWorkers;

    @Autowired
    private CsvParser csvParser;

    @Bean
    @Primary
    PlatformTransactionManager getTransactionManager(
            @Qualifier("transactionManager") PlatformTransactionManager platform) {
        return platform;
    }

    @Bean
    public JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor(JobRegistry jobRegistry) {
        JobRegistryBeanPostProcessor postProcessor = new JobRegistryBeanPostProcessor();
        postProcessor.setJobRegistry(jobRegistry);
        return postProcessor;
    }

    @Bean
    @Profile("worker")
    public DeployerStepExecutionHandler stepExecutionHandler(JobExplorer jobExplorer, JobRepository jobRepository,
                                                             ConfigurableApplicationContext context) {

        return new DeployerStepExecutionHandler(context, jobExplorer, jobRepository);
    }

    public String[] getAllFiles(String fileName) {
        String[] contents = fileName.split(",");
        for (String con : contents) {
            log.info("Content : " + con);
        }
        return contents;
    }


    public List<List<String>> splitPayLoad(String[] array, int partitionSize) {
        if (partitionSize <= 0)
            return Collections.emptyList();
        int rest = array.length % partitionSize;
        int chunks = array.length / partitionSize + (rest > 0 ? 1 : 0);
        String[][] arrays = new String[chunks][];
        for (int i = 0; i < (rest > 0 ? chunks - 1 : chunks); i++) {
            arrays[i] = Arrays.copyOfRange(array, i * partitionSize, i * partitionSize + partitionSize);
        }
        if (rest > 0) {
            arrays[chunks - 1] = Arrays.copyOfRange(array, (chunks - 1) * partitionSize,
                    (chunks - 1) * partitionSize + rest);
        }
        List<List<String>> list = new ArrayList<>();
        for (String[] arr : arrays) {
            list.add(Arrays.asList(arr));
        }
        return list;
    }


    public List<Map<String, Integer>> getStartingIndex(String fileName, int partitionSize) {
        int numberOfLines = 0;
        try {
            numberOfLines = csvParser.countLineFast(fileName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        int linesPerWorker = (int) Math.ceil(numberOfLines / (double) partitionSize);
        List<Map<String, Integer>> index = new ArrayList<>();
        for (int i = 0; i < partitionSize; i++) {
            Map<String, Integer> map = new HashMap<>();
            map.put("startingIndex", (linesPerWorker * i) + 2);
            map.put("endingIndex", map.get("startingIndex") + linesPerWorker - 1);
            index.add(map);
        }
        return index;
    }


    @Bean
    public PartitionHandler partitionHandler(TaskLauncher taskLauncher, JobExplorer jobExplorer,
                                             Environment environment, DelegatingResourceLoader delegatingResourceLoader, TaskRepository taskRepository) {
        Resource resource = delegatingResourceLoader.getResource(jarLocation);

        DeployerPartitionHandler partitionHandler = new DeployerPartitionHandler(taskLauncher, jobExplorer, resource,
                "workerStep", taskRepository);
        List<String> commandLineArguments = new ArrayList<>(5);
        commandLineArguments.add("--spring.profiles.active=worker");
        commandLineArguments.add("--spring.cloud.task.initialize.enable=false");
        commandLineArguments.add("--spring.batch.initializer.enabled=false");
        commandLineArguments.add("--spring.cloud.task.closecontextEnabled=true");
        commandLineArguments.add("--logging.level=DEBUG");
        partitionHandler.setCommandLineArgsProvider(new PassThroughCommandLineArgsProvider(commandLineArguments));
        partitionHandler.setEnvironmentVariablesProvider(new SimpleEnvironmentVariablesProvider(environment));
        partitionHandler.setMaxWorkers(maxWorkers);
        partitionHandler.setApplicationName("BatchApplicationWorker");
        return partitionHandler;
    }

    @Bean
    @StepScope
    public Partitioner partitioner(@Value("#{jobParameters['inputFiles']}") String file,
                                   @Value("#{jobParameters['partitionSize']}") String partitionSizeInput) {
        int partitionSize = Integer.parseInt(partitionSizeInput);
        return new Partitioner() {
            public Map<String, ExecutionContext> partition(int gridSize) {
                Map<String, ExecutionContext> partitions = new HashMap<>();
                String[] files = getAllFiles(file);
                if (files.length == 1) {
                    List<Map<String, Integer>> partitionPayloads = getStartingIndex(files[0], partitionSize);
                    int size = partitionPayloads.size();
                    for (int i = 0; i < size; i++) {
                        ExecutionContext executionContext = new ExecutionContext();
                        executionContext.put("partitionNumber", i);
                        executionContext.put("fileName", files[0]);
                        executionContext.put("partitionPayLoad", (Map<String, Integer>) (partitionPayloads.get(i)));
                        partitions.put("partition" + i, executionContext);
                    }
                } else {
                    log.info("Multireader");
                    List<List<String>> partitionPayloads = splitPayLoad(files, partitionSize);
                    int size = partitionPayloads.size();
                    for (int i = 0; i < size; i++) {
                        ExecutionContext executionContext = new ExecutionContext();
                        executionContext.put("partitionNumber", i);
                        executionContext.put("partitionPayLoad", new ArrayList<>(partitionPayloads.get(i)));
                        partitions.put("partition" + i, executionContext);
                    }
                }
                return partitions;
            }
        };
    }

    @Bean
    public Step masterStep(Step workerStep, PartitionHandler partitionHandler) {
        return this.stepBuilderFactory.get("masterStep").partitioner(workerStep.getName(), partitioner(null, null))
                .step(workerStep).partitionHandler(partitionHandler).build();
    }

    @Bean
    public Step workerStep(CustomProcessor customProcessor) {
        return this.stepBuilderFactory.get("workerStep").<Document, Document>chunk(500).reader(getReader(null))
                .processor(customProcessor).writer(writer(null)).build();
    }

    @Bean
    public Job batchJob(Step masterStep, JobExecutionListnerClass jobExecutionListnerClass,
                        JobBuilderFactory jobBuilderFactory) {
        return jobBuilderFactory.get("batchJob").start(masterStep).listener(jobExecutionListnerClass).build();
    }


    @Bean
    @StepScope
    public CustomMultiFileReader multiFIleReader(@Value("#{stepExecutionContext['partitionPayLoad']}") List<String> payload) {
        return new CustomMultiFileReader(payload);
    }


    @Bean
    @StepScope
    public CustomFileReader customFileReader(@Value("#{stepExecutionContext['partitionPayLoad']}") Map<String, Integer> index,
                                             @Value("#{stepExecutionContext['fileName']}") String fileName) {
        return new CustomFileReader(fileName, index);
    }


    @Bean
    @StepScope
    public ItemStreamReader<Document> getReader(@Value("#{jobParameters['inputFiles']}") String file) {
        String[] files = getAllFiles(file);
        if (files.length == 1) {
            return customFileReader(null, null);
        }
        return multiFIleReader(null);
    }


    @Bean
    @StepScope
    public CustomWriter writer(@Value("#{jobParameters['collectionName']}") String collectionName) {
        return new CustomWriter(collectionName);
    }

    @Bean
    public AppTaskListener appTaskListener() {
        return new AppTaskListener();
    }

}
