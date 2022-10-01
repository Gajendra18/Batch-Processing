import { jobParameters } from "./jobParameters";

export class JobExecution{
    jobInstanceID:number;
    jobName: String;
    status: String;
    createTime: String;
    endTime: String;
    jobParams: jobParameters;
}