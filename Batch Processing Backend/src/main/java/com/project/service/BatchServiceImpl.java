package com.project.service;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.constants.Constants;
import com.project.constants.HealthConstants;
import com.project.entity.*;
import com.project.exception.BatchException;
import com.project.quartzjobconfig.QuartzJob;
import com.project.repository.WorkerRepository;
import com.project.utility.BatchPartitionHelper;
import com.project.utility.validatorService;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.quartz.*;
import org.quartz.impl.matchers.GroupMatcher;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.*;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.*;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.SampleOperation;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@Slf4j
@Service(value = "batchService")
public class BatchServiceImpl implements BatchService {

    @Autowired
    private JobExplorer explorer;

    @Autowired
    private Job job;

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private Scheduler scheduler;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private AmazonS3 s3Client;

    @Autowired
    private WorkerRepository workerRepository;

    @Autowired
    private validatorService validatorService;

    @Value("${application.bucket.name}")
    private String bucketName;

    @Autowired
    private BatchPartitionHelper batchPartitionHelper;


    @Override
    public Long runJob(List<MultipartFile> multipartFile, String params) throws BatchException {

        ObjectMapper objectMapper = new ObjectMapper();
        JobParams jobParamsObj;
        try {
            jobParamsObj = objectMapper.readValue(params, JobParams.class);
        } catch (IOException e) {
            throw new BatchException(e.getMessage());
        }


        int numberOfWorkers;
        if (multipartFile.size() == 1) {
            List<Map<String, Integer>> partitionPayloads = batchPartitionHelper.getStartingIndex(multipartFile.get(0), jobParamsObj.getPartitionSize());
            if (partitionPayloads == null) {
                throw new BatchException("Service.EMPTY_FILE");
            }
            numberOfWorkers = partitionPayloads.size();
        } else {
            String[] files = multipartFile.stream().map(MultipartFile::getName).toArray(String[]::new);
            List<List<String>> partitionPayloads = batchPartitionHelper.splitPayLoad(files, jobParamsObj.getPartitionSize());
            numberOfWorkers = partitionPayloads.size();
        }

        validatorService.checkJobName(jobParamsObj.getJobName());
        validatorService.checkCollectionName(jobParamsObj.getCollectionName());

        log.info("Validations complete");
        String inputFiles = multipartFile.stream().map(this::uploadFile).collect(Collectors.joining(","));
        log.info("Files uploaded");

        jobParamsObj.setInputSource(inputFiles);
        JobExecution jobExecution;
        JobParameters jobParameters = buildParameters(jobParamsObj, numberOfWorkers);
        try {
            jobExecution = jobLauncher.run(job, jobParameters);
        } catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException |
                 JobParametersInvalidException e) {
            throw new RuntimeException(e);
        }
        return jobExecution.getJobId();
    }


    @Override
    public Long restartJob(Long jobId) throws BatchException {
        Long jobExecutionId;
        try {
            jobExecutionId = jobOperator.getExecutions(jobId).get(0);
            return jobOperator.restart(jobExecutionId);
        } catch (JobInstanceAlreadyCompleteException | NoSuchJobExecutionException | NoSuchJobException |
                 JobRestartException | JobParametersInvalidException | NoSuchJobInstanceException e) {
            throw new BatchException(e.getMessage());
        }
    }

    @Override
    public Boolean stopJob(Long jobInstanceId) throws BatchException {
        try {
            Optional<JobExecution> optionalJobExecution = Optional.ofNullable(explorer.getJobExecution(jobOperator.getExecutions(jobInstanceId).get(0)));
            JobExecution jobExecution = optionalJobExecution
                    .orElseThrow(() -> new BatchException("Service.JOB_EXECUTION_NOT_FOUND"));

            if( !jobExecution.getStatus().equals(BatchStatus.STARTED) && jobExecution.getStepExecutions().size() == 0){
                jobExecution.setStatus(BatchStatus.STOPPED);
                jobRepository.update(jobExecution);
                updateInCompleteWorkersWithId(jobExecution.getId());
                return true;
            }else{
                return jobOperator.stop(jobOperator.getExecutions(jobInstanceId).get(0));
            }
        } catch (NoSuchJobExecutionException | JobExecutionNotRunningException | NoSuchJobInstanceException e) {
            throw new BatchException(e.getMessage());
        }
    }


    @Override
    public void stopJobWithExecutionId(Long jobExecutionId) throws BatchException {
        Optional<JobExecution> optionalJobExecution = Optional.ofNullable(explorer.getJobExecution(jobExecutionId));
        JobExecution jobExecution = optionalJobExecution
                .orElseThrow(() -> new BatchException("Service.JOB_EXECUTION_NOT_FOUND"));

        if (jobExecution.getStatus().equals(BatchStatus.STARTED) || jobExecution.getStatus().equals(BatchStatus.STOPPING) || jobExecution.getStatus().equals(BatchStatus.UNKNOWN)) {
            int readCount = 0;
            int writeCount = 0;
            StepExecution masterStep = null;
            jobExecution.setEndTime(new Date());
            long completedSteps = jobExecution.getStepExecutions().stream().filter(stepExecution -> !stepExecution.getStepName().contains("masterStep") && stepExecution.getStatus().equals(BatchStatus.COMPLETED)).count();
            int count = 0;
            for (StepExecution se : jobExecution.getStepExecutions()) {
                if (se.getStepName().contains("masterStep")) {
                    masterStep = se;
                } else {
                    readCount += se.getReadCount();
                    writeCount += se.getWriteCount();
                    if (se.getStatus().equals(BatchStatus.STARTED) || se.getStatus().equals(BatchStatus.STARTING)) {
                        se.setEndTime(new Date());
                        se.setStatus(BatchStatus.FAILED);
                        se.setExitStatus(ExitStatus.FAILED);
                        jobRepository.update(se);
                    }
                    count++;
                }
            }
            if (masterStep != null) {
                masterStep.setReadCount(readCount);
                masterStep.setWriteCount(writeCount);
                masterStep.setEndTime(new Date());
                if (completedSteps != 0 && completedSteps == count) {
                    masterStep.setStatus(BatchStatus.COMPLETED);
                    masterStep.setExitStatus(ExitStatus.COMPLETED);
                    jobExecution.setStatus(BatchStatus.COMPLETED);
                    jobExecution.setExitStatus(ExitStatus.COMPLETED);
                } else {
                    masterStep.setStatus(BatchStatus.FAILED);
                    masterStep.setExitStatus(ExitStatus.FAILED);
                    jobExecution.setStatus(BatchStatus.FAILED);
                    jobExecution.setExitStatus(ExitStatus.FAILED);
                }
                jobRepository.update(masterStep);
            }else{
                jobExecution.setStatus(BatchStatus.FAILED);
                jobExecution.setExitStatus(ExitStatus.FAILED);
            }
            jobRepository.update(jobExecution);
        }
        updateInCompleteWorkersWithId(jobExecutionId);
    }

    @Override
    public void startupClean() throws BatchException {
        List<JobInstance> instanceIds = explorer.getJobInstances(job.getName(), 0, Integer.MAX_VALUE);
        for (JobInstance id : instanceIds) {
            Long executionId;
            try {
                executionId = jobOperator.getExecutions(id.getInstanceId()).get(0);
            } catch (NoSuchJobInstanceException e) {
                throw new BatchException(e.getMessage());
            }
            stopJobWithExecutionId(executionId);
        }
        updateInCompleteWorkers();
    }



    @Override
    public List<BatchJob> getAllJobs() throws BatchException {
        List<JobInstance> instanceIds = explorer.getJobInstances(job.getName(), 0, Integer.MAX_VALUE);
        List<BatchJob> allJobs = new ArrayList<>();

        for (JobInstance id : instanceIds) {
            Long executionId;
            try {
                executionId = jobOperator.getExecutions(id.getInstanceId()).get(0);
            } catch (NoSuchJobInstanceException e) {
                throw new BatchException(e.getMessage());
            }
            Optional<JobExecution> optionalJobExecution = Optional.ofNullable(explorer.getJobExecution(executionId));
            JobExecution jobExecution = optionalJobExecution
                    .orElseThrow(() -> new BatchException("Service.JOB_EXECUTION_NOT_FOUND"));

            BatchJob batchJob = new BatchJob();
            batchJob.setJobID(id.getInstanceId());
            batchJob.setJobName(jobExecution.getJobParameters().getString(Constants.JOB_NAME));
            batchJob.setStatus(jobExecution.getStatus());
            batchJob.setCreateTime(jobExecution.getCreateTime());
            batchJob.setEndTime(jobExecution.getEndTime());
            allJobs.add(batchJob);
        }
        return allJobs;
    }


    @Override
    public List<BatchJob> getAllJobExecutions(Long jonInstanceId) throws BatchException {
        List<BatchJob> allJobs = new ArrayList<>();
        List<Long> allExecutionIds;
        try {
            allExecutionIds = jobOperator.getExecutions(jonInstanceId);
        } catch (NoSuchJobInstanceException e) {
            throw new BatchException(e.getMessage());
        }
        for (Long executionId : allExecutionIds) {

            Optional<JobExecution> optionalJobExecution = Optional.ofNullable(explorer.getJobExecution(executionId));
            JobExecution jobExecution = optionalJobExecution
                    .orElseThrow(() -> new BatchException("Service.JOB_EXECUTION_NOT_FOUND"));

            BatchJob batchJob = new BatchJob();
            JobParams jobParams = new JobParams();

            batchJob.setJobID(jobExecution.getId());
            batchJob.setJobName(jobExecution.getJobParameters().getString(Constants.JOB_NAME));
            batchJob.setStatus(jobExecution.getStatus());
            batchJob.setCreateTime(jobExecution.getCreateTime());
            batchJob.setEndTime(jobExecution.getEndTime());

            jobParams.setInputSource(jobExecution.getJobParameters().getString(Constants.INPUT_FILES));
            jobParams.setPartitionSize(
                    Integer.parseInt(jobExecution.getJobParameters().getString(Constants.PARTITION_SIZE)));
            jobParams.setMailRecipients(jobExecution.getJobParameters().getString(Constants.MAIL_RECIPIENTS));
            jobParams.setJobDescription(jobExecution.getJobParameters().getString(Constants.JOB_DESCRIPTION));
            jobParams.setJobName(jobExecution.getJobParameters().getString(Constants.JOB_NAME));
            jobParams.setRestart(Boolean.valueOf(jobExecution.getJobParameters().getString(Constants.JOB_RESTART)));
            jobParams.setCollectionName(jobExecution.getJobParameters().getString(Constants.COLLECTION_NAME));
            batchJob.setJobParams(jobParams);
            allJobs.add(batchJob);
        }
        return allJobs;
    }

    @Override
    public List<WorkerNode> getAllWorkerNodes(Long jobExecutionId) throws BatchException {
        List<WorkerNode> allWorkers = new ArrayList<>();

        Map<Long, String> stepSummary;
        try {
            stepSummary = jobOperator.getStepExecutionSummaries(jobExecutionId);
        } catch (NoSuchJobExecutionException e) {
            throw new BatchException(e.getMessage());
        }
        for (var entry : stepSummary.entrySet()) {

            Optional<StepExecution> optionalstepExecution = Optional
                    .ofNullable(explorer.getStepExecution(jobExecutionId, entry.getKey()));
            StepExecution stepExecution = optionalstepExecution
                    .orElseThrow(() -> new BatchException("Service.STEP_EXECUTION_NOT_FOUND"));

            WorkerNode workerNode = new WorkerNode();
            workerNode.setStepName(stepExecution.getStepName());
            workerNode.setStatus(stepExecution.getStatus());
            workerNode.setStartTime(stepExecution.getStartTime());
            workerNode.setEndTime(stepExecution.getEndTime());
            workerNode.setReadCount(stepExecution.getReadCount());
            workerNode.setWriteCount(stepExecution.getWriteCount());
            allWorkers.add(workerNode);
        }
        return allWorkers;
    }

    @Override
    public WorkerNode getWorkerNode(Long jobExecutionId, String partitionName) throws BatchException {
        Map<Long, String> stepSummary;
        try {
            stepSummary = jobOperator.getStepExecutionSummaries(jobExecutionId);
        } catch (NoSuchJobExecutionException e) {
            throw new BatchException(e.getMessage());
        }
        for (var entry : stepSummary.entrySet()) {

            Optional<StepExecution> optionalstepExecution = Optional
                    .ofNullable(explorer.getStepExecution(jobExecutionId, entry.getKey()));
            StepExecution stepExecution = optionalstepExecution
                    .orElseThrow(() -> new BatchException("Service.STEP_EXECUTION_NOT_FOUND"));

            if (stepExecution.getStepName().endsWith(partitionName)) {
                WorkerNode workerNode = new WorkerNode();
                workerNode.setStepName(stepExecution.getStepName());
                workerNode.setStatus(stepExecution.getStatus());
                workerNode.setStartTime(stepExecution.getStartTime());
                workerNode.setEndTime(stepExecution.getEndTime());
                workerNode.setReadCount(stepExecution.getReadCount());
                workerNode.setWriteCount(stepExecution.getWriteCount());
                return workerNode;
            }
        }
        return null;
    }

    @Override
    public List<String> getWorkerLogs(Long jobExecutionID, String partitionName) throws BatchException {
        List<String> list = new ArrayList<>();
        log.info("partition Name" + partitionName);
        log.info("jobExecutionID : " + jobExecutionID);
        String fileName = workerRepository.findExternalExecutionId(jobExecutionID, partitionName);
        log.info(fileName);
        Scanner scanner;
        try {
            scanner = new Scanner(new File(fileName));
        } catch (FileNotFoundException e) {
            throw new BatchException(e.getMessage());
        }
        while (scanner.hasNextLine()) {
            list.add(scanner.nextLine());
        }
        scanner.close();
        return list;

    }

    @Override
    public Boolean scheduleJob(List<MultipartFile> multipartFile, String params) throws BatchException {

        ObjectMapper objectMapper = new ObjectMapper();
        JobScheduleDetails scheduleDetailsObj;
        try {
            scheduleDetailsObj = objectMapper.readValue(params, JobScheduleDetails.class);
        } catch (IOException e) {
            throw new BatchException(e.getMessage());
        }

        long numberOfWorkers;
        if (multipartFile.size() == 1) {
            List<Map<String, Integer>> partitionPayloads = batchPartitionHelper.getStartingIndex(multipartFile.get(0), scheduleDetailsObj.getPartitionSize());
            numberOfWorkers = partitionPayloads.size();
        } else {
            String[] files = multipartFile.stream().map(MultipartFile::getName).toArray(String[]::new);
            List<List<String>> partitionPayloads = batchPartitionHelper.splitPayLoad(files, scheduleDetailsObj.getPartitionSize());
            numberOfWorkers = partitionPayloads.size();
        }

        validatorService.checkJobName(scheduleDetailsObj.getJobName());
        validatorService.checkCollectionName(scheduleDetailsObj.getCollectionName());

        log.info("Validations complete");

        String inputFiles = multipartFile.stream().map(this::uploadFile).collect(Collectors.joining(","));
        log.info("Files uploaded");



        scheduleDetailsObj.setInputSource(inputFiles);

        JobDetail detail = buildJobDetail(scheduleDetailsObj.getJobName(), scheduleDetailsObj.getJobGroup(),
                scheduleDetailsObj.getJobDescription());
        detail.getJobDataMap().put(Constants.INPUT_FILES, scheduleDetailsObj.getInputSource());
        detail.getJobDataMap().put(Constants.PARTITION_SIZE, scheduleDetailsObj.getPartitionSize());
        detail.getJobDataMap().put(Constants.MAIL_RECIPIENTS, scheduleDetailsObj.getMailRecipients());
        detail.getJobDataMap().put(Constants.JOB_NAME, scheduleDetailsObj.getJobName());
        detail.getJobDataMap().put(Constants.JOB_DESCRIPTION, scheduleDetailsObj.getJobDescription());
        detail.getJobDataMap().put(Constants.COLLECTION_NAME, scheduleDetailsObj.getCollectionName());
        detail.getJobDataMap().put(Constants.JOB_RESTART, scheduleDetailsObj.getRestart());
        detail.getJobDataMap().put("WORKERS", numberOfWorkers);
        Trigger trigger = buildJobTrigger(detail, scheduleDetailsObj.getCronExpression());



        try {
            scheduler.scheduleJob(detail, trigger);
            return true;
        } catch (SchedulerException e) {
            throw  new BatchException("Service.INVALID_CRON_EXPRESSION");
        }
    }

    private JobDetail buildJobDetail(String jobName, String jobGroup, String jobDesciption) {
        return JobBuilder.newJob(QuartzJob.class).withIdentity(jobName + ":" + UUID.randomUUID(), jobGroup)
                .withDescription(jobDesciption).storeDurably().build();
    }

    private Trigger buildJobTrigger(JobDetail jobDetail, String cronExpression) {
        return TriggerBuilder.newTrigger().forJob(jobDetail).withIdentity(jobDetail.getKey().getName(), "job-triggers")
                .withDescription("send Trigger").withSchedule(CronScheduleBuilder.cronSchedule(cronExpression)).build();
    }

    @Override
    public List<JobScheduleDetails> getScheduledJobs() throws BatchException {
        List<JobScheduleDetails> listOfScheduledJobs = new ArrayList<>();

        try {
            List<String> groupNames = scheduler.getJobGroupNames();
            for (String groupName : groupNames) {
                try {
                    for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.jobGroupEquals(groupName))) {
                        if (!scheduler.getTriggersOfJob(jobKey).isEmpty()) {
                            Trigger trigger;
                            try {
                                trigger = scheduler.getTriggersOfJob(jobKey).get(0);
                            } catch (SchedulerException e) {
                                throw new BatchException(e.getMessage());
                            }

                            String jobName = jobKey.getName();
                            String jobGroup = jobKey.getGroup();

                            JobDetail detail = scheduler.getJobDetail(jobKey);

                            JobDataMap dataMap = detail.getJobDataMap();
                            String description = detail.getDescription();
                            if (dataMap.size() <  5 ) continue;
                            JobScheduleDetails batchJob = new JobScheduleDetails();
                            batchJob.setInputSource(dataMap.getString(Constants.INPUT_FILES));
                            batchJob.setMailRecipients(dataMap.getString(Constants.MAIL_RECIPIENTS));
                            batchJob.setPartitionSize(dataMap.getInt(Constants.PARTITION_SIZE));
                            batchJob.setCronExpression(trigger.getNextFireTime().toString());
                            batchJob.setJobName(jobName);
                            batchJob.setCollectionName(dataMap.getString(Constants.COLLECTION_NAME));
                            batchJob.setJobGroup(jobGroup);
                            batchJob.setJobDescription(description);
                            batchJob.setRestart(dataMap.getBoolean(Constants.JOB_RESTART));

                            listOfScheduledJobs.add(batchJob);

                        } else {
                            scheduler.deleteJob(jobKey);
                        }
                    }
                } catch (SchedulerException e) {
                    throw new BatchException(e.getMessage());
                }
            }
        } catch (SchedulerException e) {
            throw new BatchException(e.getMessage());
        }
        return listOfScheduledJobs;
    }

    @Override
    public Boolean unscheduleJob(String jobName, String jobGroup) throws BatchException {
        JobKey jobKey = new JobKey(jobName, jobGroup);
        try {
            return scheduler.deleteJob(jobKey);
        } catch (SchedulerException e) {
            throw new BatchException(e.getMessage());
        }
    }

    public String uploadFile(MultipartFile file) {
        File fileObject = convertMultiPartFileToFile(file);
        String fileName = System.currentTimeMillis() + "_" + file.getOriginalFilename();
        s3Client.putObject(new PutObjectRequest(bucketName, fileName, fileObject));
        log.info("File Objected Deleted :" + fileObject.delete());
        return fileName;
    }


    private File convertMultiPartFileToFile(MultipartFile file) {
        File convertedFile = new File(Objects.requireNonNull(file.getOriginalFilename()));
        try (FileOutputStream fos = new FileOutputStream(convertedFile)) {
            fos.write(file.getBytes());
        } catch (IOException e) {
            log.error("Error converting multipartFile to file", e);
        }
        return convertedFile;
    }

    public byte[] downloadCsv(Long jobId) throws BatchException {
        Long jobExecutionId;
        try {
            jobExecutionId = jobOperator.getExecutions(jobId).get(0);
        } catch (NoSuchJobInstanceException e) {
            throw new BatchException(e.getMessage());
        }

        Optional<JobExecution> optionalJobExecution = Optional.ofNullable(explorer.getJobExecution(jobExecutionId));
        JobExecution jobExecution = optionalJobExecution
                .orElseThrow(() -> new BatchException("Service.JOB_EXECUTION_NOT_FOUND"));

        String fileNames = jobExecution.getJobParameters().getString(Constants.INPUT_FILES);
        String[] fileNamesArray = fileNames.split(",");

        byte[] result = null;

        try (ByteArrayOutputStream fos = new ByteArrayOutputStream();
             ZipOutputStream zipOut = new ZipOutputStream(fos);) {
            for (String file : fileNamesArray) {
                S3Object s3Object = s3Client.getObject(bucketName, file);
                try (InputStream fis = s3Object.getObjectContent();) {
                    String[] names = file.split("_");
                    String filename = Stream.of(names).skip(1).collect(Collectors.joining(""));
                    ZipEntry zipEntry = new ZipEntry(filename);
                    zipOut.putNextEntry(zipEntry);
                    IOUtils.copy(fis, zipOut);
                }
                s3Object.close();
            }
            result = fos.toByteArray();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return result;
    }


    @Transactional
    public void updateInCompleteWorkers() {
        List<WorkerInfo> incompleteWorkers = workerRepository.findIncompleteWorkers();
        for (WorkerInfo worker : incompleteWorkers) {
            worker.setStatus(HealthConstants.FAILED);
            workerRepository.save(worker);
        }
    }


    @Transactional
    public void updateInCompleteWorkersWithId(long jobExecutionId) {
        List<WorkerInfo> incompleteWorkers = workerRepository.findIncompleteWorkersForId(jobExecutionId);
        for (WorkerInfo worker : incompleteWorkers) {
            worker.setStatus(HealthConstants.FAILED);
            workerRepository.save(worker);
        }
    }


    @Transactional
    public void updateWorkers(long jobExecutionId, String taskName, long taskExecutionId, String logName) {
        WorkerInfo worker = workerRepository.findStatus(jobExecutionId, taskName);
        worker.setTaskExecutionId(taskExecutionId);
        worker.setExternalExecutionId(logName);
        worker.setStatus(HealthConstants.INITIALIZED);
        worker.setLastUpdated(LocalDateTime.now());
    }


    public Map<String, String> getCollectionSchema(Long jobId) throws BatchException {
        Long jobExecutionId;
        try {
            jobExecutionId = jobOperator.getExecutions(jobId).get(0);
        } catch (NoSuchJobInstanceException e) {
            throw new RuntimeException(e);
        }
        Optional<JobExecution> optionalJobExecution = Optional.ofNullable(explorer.getJobExecution(jobExecutionId));
        JobExecution jobExecution = optionalJobExecution
                .orElseThrow(() -> new BatchException("Service.JOB_EXECUTION_NOT_FOUND"));

        int writeCount = 0;
        for (StepExecution se : jobExecution.getStepExecutions()) {
            if (!se.getStepName().contains("masterStep")) {
                writeCount += se.getWriteCount();
            }
        }
        if (writeCount != 0){
            Map<String, String> schema = new HashMap<>();
            try{
                String collectionName = jobExecution.getJobParameters().getString(Constants.COLLECTION_NAME);
                SampleOperation matchStage = Aggregation.sample(1);
                Aggregation aggregation = Aggregation.newAggregation(matchStage);
                AggregationResults<Document> output = mongoTemplate.aggregate(aggregation, collectionName, Document.class);
                List<Document> results =  output.getMappedResults();

                for(Document doc: results){
                    for (String key : doc.keySet()){
                        schema.put(key, doc.get(key).getClass().getSimpleName());
                    }
                }
            } catch (Exception e) {
                return null;
            }
            return schema;
        }else{
            return null;
        }
    }


    public JobParameters buildParameters(JobParams jobParams, int numberOfWorkers) {
        Map<String, JobParameter> maps = new HashMap<>();
        maps.put(Constants.TIME, new JobParameter(System.currentTimeMillis()));
        maps.put(Constants.INPUT_FILES, new JobParameter(jobParams.getInputSource()));
        maps.put(Constants.PARTITION_SIZE, new JobParameter(Integer.toString(jobParams.getPartitionSize())));
        maps.put(Constants.MAIL_RECIPIENTS, new JobParameter(jobParams.getMailRecipients()));
        maps.put(Constants.JOB_NAME, new JobParameter(jobParams.getJobName()));
        maps.put(Constants.JOB_DESCRIPTION, new JobParameter(jobParams.getJobDescription()));
        maps.put(Constants.COLLECTION_NAME, new JobParameter((jobParams.getCollectionName())));
        maps.put(Constants.JOB_RESTART, new JobParameter(Boolean.toString(jobParams.getRestart())));
        maps.put("WORKERS", new JobParameter(Integer.toString(numberOfWorkers)));
        return new JobParameters(maps);
    }


}