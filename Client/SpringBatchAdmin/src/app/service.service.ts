import { Injectable } from '@angular/core';
import { HttpClient, HttpEvent } from '@angular/common/http';
import { Observable } from 'rxjs';
import { Job } from './Entities/Job';
import { map } from 'rxjs/operators';
import { JobExecution } from './Entities/JobExecution';
import { Steps } from './Entities/Steps';
import { jobParameters } from './Entities/jobParameters';
import { jobScheduler } from './Entities/jobScheduler';


@Injectable({
  providedIn: 'root'
})
export class ServiceService {

  constructor(private http: HttpClient) { }

  allJobsUrl = "http://localhost:8080/batch/jobs";
  getAllJobs(): Observable<Job[]> {
    return this.http.get<Job[]>(this.allJobsUrl);
  }

  allExecutions = "http://localhost:8080/batch/jobExecutions?jobInstanceID=";
  getAllExecutions(jobInstanceID): Observable<JobExecution[]> {
    return this.http.get<JobExecution[]>(this.allExecutions + jobInstanceID);
  }

  allWorkers = "http://localhost:8080/batch/workerNodes?jobExecutionID=";
  getAllWorkers(jobExecutionID): Observable<Steps[]> {
    return this.http.get<Steps[]>(this.allWorkers + jobExecutionID);
  }

  stopJobUrl = "http://localhost:8080/batch/stop?id=";
  stopJob(jobId): Observable<boolean> {
    return this.http.get<boolean>(this.stopJobUrl + jobId);
  }

  restartJobUrl = "http://localhost:8080/batch/restart?jobId=";
  restartJob(jobId): Observable<boolean> {
    return this.http.get<boolean>(this.restartJobUrl + jobId);
  }

  startJobUrl = "http://localhost:8080/batch/run";
  startJob(files, jobParams: jobParameters): Observable<Boolean> {
    let data = new FormData();
    for (let index = 0; index < files.length; index++) {
      data.append("files", files[index])
    }

    data.append('jobParams', JSON.stringify(jobParams));


    data.forEach((value, key) => {
      console.log(key + " " + value)
    });
    // return null;
    return this.http.post<Boolean>(this.startJobUrl, data);
  }

  scheduleJobUrl = "http://localhost:8080/batch/schedule";
  scheduleJob(files, jobParams: jobScheduler): Observable<boolean> {
    let data = new FormData();
    for (let index = 0; index < files.length; index++) {
      data.append("files", files[index])
    }

    data.append('jobParams', JSON.stringify(jobParams));


    data.forEach((value, key) => {
      console.log(key + " " + value)
    });

    return this.http.post<boolean>(this.scheduleJobUrl, data);
  }

  scheduledJobsUrl = "http://localhost:8080/batch/scheduledJobs";
  getScheduled(): Observable<[]> {
    return this.http.get<[]>(this.scheduledJobsUrl);
  }

  unscheduleJobUrl = "http://localhost:8080/batch/unschedule?";
  unscheduleJob(jobName, jobGroup): Observable<boolean> {
    let url = this.unscheduleJobUrl + "jobName=" + jobName + "&jobGroup=" + jobGroup;
    console.log(url);
    return this.http.get<boolean>(url);
  }

  downloadFIlesUrl = "http://localhost:8080/batch/download?jobId=";
  downloadFIles(jobId): Observable<HttpEvent<any>> {
    let url = this.downloadFIlesUrl + jobId;
     return this.http.get(url, {
       reportProgress: true,
       observe: 'events',
       responseType: 'blob'
     });
  }


  getLogUrl = "http://localhost:8080/batch/workerLogs?";
  getWorkerLog(jobExecutionID, partitionName): Observable<[]> {
    return this.http.get<[]>(this.getLogUrl + "jobExecutionID=" + jobExecutionID + "&partitionName=" + partitionName);
  }

  getSchemaUrl = "http://localhost:8080/batch/schema?jobId=";
  getSchema(jobExecutionID): Observable<Map<String, String>> {
    return this.http.get<Map<String, String>>(this.getSchemaUrl + jobExecutionID);
  }
}
