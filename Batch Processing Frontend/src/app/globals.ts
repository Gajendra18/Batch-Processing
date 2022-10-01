import { Injectable } from '@angular/core';
import { Status } from './Entities/status';

Injectable()
export class Globals{
    jobExecutions = new Map<number,Status>();
}