import { jobParameters } from "./jobParameters";

export class jobScheduler extends jobParameters{
    cronExpression: String;
    jobGroup: String;
}