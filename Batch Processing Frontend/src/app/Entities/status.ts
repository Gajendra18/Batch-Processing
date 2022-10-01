import { Partition } from "./partition";

export class Status{
    jobExecutionId: number;
    status:Map<String,Partition> = new Map<String,Partition>();
}