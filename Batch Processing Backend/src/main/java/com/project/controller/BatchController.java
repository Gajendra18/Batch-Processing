package com.project.controller;

import com.project.entity.BatchJob;
import com.project.entity.JobScheduleDetails;
import com.project.entity.WorkerNode;
import com.project.exception.BatchException;
import com.project.service.BatchService;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.SampleOperation;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/batch")
@CrossOrigin(origins = "http://localhost:4200")
public class BatchController {

    @Autowired
    private BatchService batchService;

    @Autowired
    private MongoTemplate mongoTemplate;





    @PostMapping("/run")
    public ResponseEntity<Long> run(@RequestParam("files") List<MultipartFile> multipartFile,
                                    @RequestParam("jobParams") String jobParams) throws BatchException {
        long jobId = batchService.runJob(multipartFile, jobParams);
        return new ResponseEntity<>(jobId, HttpStatus.ACCEPTED);
    }

    @GetMapping("/restart")
    public ResponseEntity<Long> restart(@RequestParam Long jobId) throws BatchException {
        Long restartId = batchService.restartJob(jobId);
        return new ResponseEntity<>(restartId, HttpStatus.OK);
    }

    @GetMapping("/stop")
    public ResponseEntity<Boolean> stopJob(@RequestParam Long id) throws BatchException {
        Boolean status = batchService.stopJob(id);
        return new ResponseEntity<>(status, HttpStatus.OK);
    }

    @GetMapping("/jobs")
    public ResponseEntity<List<BatchJob>> getAllJobs() throws BatchException {
        List<BatchJob> allJobs = batchService.getAllJobs();
        return new ResponseEntity<>(allJobs, HttpStatus.OK);
    }

    @GetMapping("/jobExecutions")
    public ResponseEntity<List<BatchJob>> getAllJobExecutions(@RequestParam Long jobInstanceID) throws BatchException {
        List<BatchJob> allJobExecutions = batchService.getAllJobExecutions(jobInstanceID);
        return new ResponseEntity<>(allJobExecutions, HttpStatus.OK);
    }

    @GetMapping("/schema")
    public ResponseEntity<Map<String, String>> collectionSchema(@RequestParam Long jobId) throws BatchException {
        Map<String, String> schema = batchService.getCollectionSchema(jobId);
        return new ResponseEntity<>(schema, HttpStatus.OK);
    }

    @GetMapping("/workerNodes")
    public ResponseEntity<List<WorkerNode>> getAllWorkerNodes(@RequestParam Long jobExecutionID) throws BatchException {
        List<WorkerNode> allWorkers = batchService.getAllWorkerNodes(jobExecutionID);
        return new ResponseEntity<>(allWorkers, HttpStatus.OK);
    }

    @GetMapping("/workerLogs")
    public ResponseEntity<List<String>> getWorkerLogs(@RequestParam Long jobExecutionID,
                                                      @RequestParam String partitionName) throws BatchException {
        List<String> logs = batchService.getWorkerLogs(jobExecutionID, partitionName);
        return new ResponseEntity<>(logs, HttpStatus.OK);
    }

    @PostMapping("/schedule")
    public ResponseEntity<Boolean> setSchedule(@RequestParam("files") List<MultipartFile> multipartFile,
                                               @RequestParam("jobParams") String scheduleDetails) throws BatchException {
        Boolean scheduleStatus = batchService.scheduleJob(multipartFile, scheduleDetails);
        return new ResponseEntity<>(scheduleStatus, HttpStatus.OK);
    }

    @GetMapping("/scheduledJobs")
    public ResponseEntity<List<JobScheduleDetails>> getScheduledJobs() throws BatchException {
        List<JobScheduleDetails> listOfScheduledJobs = batchService.getScheduledJobs();
        return new ResponseEntity<>(listOfScheduledJobs, HttpStatus.OK);
    }

    @GetMapping("/unschedule")
    public ResponseEntity<Boolean> unscheduleJob(@RequestParam String jobName, @RequestParam String jobGroup)
            throws BatchException {
        Boolean status = batchService.unscheduleJob(jobName, jobGroup);
        return new ResponseEntity<>(status, HttpStatus.OK);
    }

    @GetMapping("/download")
    public ResponseEntity<ByteArrayResource> downloadFile(@RequestParam Long jobId) throws BatchException, IOException {
        byte[] data = batchService.downloadCsv(jobId);
        ByteArrayResource resource = new ByteArrayResource(data);
        String fileName = "JobId - " + jobId + ".zip";
        return ResponseEntity.ok().contentLength(data.length).header("Content-type", "application/octet-stream")
                .header("Content-disposition", "attachment; filename=\"" + fileName + "\"").body(resource);
    }

}
