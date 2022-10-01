import { Component, OnInit, ViewChild } from '@angular/core';
import {MatTableDataSource} from '@angular/material/table';
import { map } from "rxjs/operators"; 
import {MatPaginator} from '@angular/material/paginator';
import { Job } from '../../Entities/Job';
import { ServiceService } from '../../service.service';

import { Subscription } from "rxjs";
import { Message } from '@stomp/stompjs';
import { RxStompService } from "@stomp/ng2-stompjs";

@Component({
  selector: 'app-dashboard',
  templateUrl: './dashboard.component.html',
  styleUrls: ['./dashboard.component.css']
})
export class DashboardComponent implements OnInit {
  listOfJobs = [];
  dataSource;
  displayedColumns;
  loading = true;
  topicSubscription: Subscription;

  @ViewChild(MatPaginator) paginator: MatPaginator;


  constructor(private rxStompService: RxStompService, private _service: ServiceService) { }

  ngOnInit(): void {
    this.initialization();
    this.topicSubscription = this.rxStompService.watch("/topic/public").subscribe((message: Message) => {
      let jsonMessage = JSON.parse(message.body);
      if (jsonMessage['status'] == "started" || jsonMessage['status'] == "completed") {
        this.initialization();
      }
    });
  }

  initialization(){
    this.getJobs().subscribe(_ => {this.dataSource = new MatTableDataSource<Job>(this.listOfJobs);
      this.loading = false;
      this.displayedColumns = ['jobID', 'jobName', 'createTime','endTime','duration', 'status','actions'];
      this.dataSource.paginator = this.paginator;  },
      (error) => {                            
        this.loading = false;
      });  
  }

  getJobs() {
    return this._service
    .getAllJobs()
    .pipe(map(
      (users) => {
        this.listOfJobs = users;
      }));
  }


log(){

}

applyFilter(event: Event) {
const filterValue = (event.target as HTMLInputElement).value;
this.dataSource.filter = filterValue.trim().toLowerCase();
}

calculateDuration(startTime:any,endTime:any){
startTime = new Date(startTime);
endTime = new Date(endTime);
let duration = new Date(0,0,0);
var seconds = endTime.getTime() - startTime.getTime();
duration.setMilliseconds(seconds);
return duration;
}

}
