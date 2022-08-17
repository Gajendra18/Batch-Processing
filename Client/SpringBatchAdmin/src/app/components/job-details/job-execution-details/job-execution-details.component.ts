import { Component, OnInit, ViewChild } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { ServiceService } from '../../../service.service';
import { MatTableDataSource } from '@angular/material/table';
import { map } from "rxjs/operators";
import { MatPaginator } from '@angular/material/paginator';
import { Steps } from '../../../Entities/Steps';
import { MatDialog } from '@angular/material/dialog';
import { SteplogsComponent } from './steplogs/steplogs.component';
import { Subscription } from "rxjs";
import { Message } from '@stomp/stompjs';
import { RxStompService } from "@stomp/ng2-stompjs";



@Component({
  selector: 'app-job-execution-details',
  templateUrl: './job-execution-details.component.html',
  styleUrls: ['./job-execution-details.component.css']
})
export class JobExecutionDetailsComponent implements OnInit {
  jobExecutionId;
  listOfWorkers = []
  dataSource;
  class = [];
  statusVar;
  liveStatus = false;
  loading = true;
  topicSubscription: Subscription;
  displayedColumns = ['stepName', 'createTime', 'endTime', 'duration', 'readCount', 'writeCount', 'status', 'logs'];;
  constructor(private route: ActivatedRoute, private _service: ServiceService, public dialog: MatDialog, private rxStompService: RxStompService) { }
  @ViewChild(MatPaginator) paginator: MatPaginator;
  ngOnInit(): void {
    let id = parseInt(this.route.snapshot.paramMap.get('jobExecutionId'));
    this.jobExecutionId = id;
    this.execute();
    this.topicSubscription = this.rxStompService.watch("/topic/public").subscribe((message: Message) => {
      let jsonMessage = JSON.parse(message.body);
      if (jsonMessage['jobExecutionId'] == this.jobExecutionId) {
        this.listOfWorkers.forEach((worker, index) => {
          if ((jsonMessage["status"] == "running" || jsonMessage["status"] == "stepCompleted"  || jsonMessage["status"] == "stepStarted") && worker.stepName == jsonMessage["workernode"].stepName) {
            this.listOfWorkers[index] = jsonMessage["workernode"];
            this.dataSource = new MatTableDataSource<Steps>(this.listOfWorkers);
            this.dataSource.paginator = this.paginator;
          } else {
            this.execute();
          }
        })
      }
    });                                   

  }

  execute() {
    this.getAllWorkers(this.jobExecutionId).subscribe(_ => {
      this.loading = false;
      this.dataSource = new MatTableDataSource<Steps>(this.listOfWorkers);
      this.dataSource.paginator = this.paginator;
    }, (error) => {                            
      this.loading = false;
    });
  }


  getAllWorkers(id) {
    return this._service
      .getAllWorkers(id)
      .pipe(map(
        (users) => {
          this.listOfWorkers = users;
          this.listOfWorkers.reverse().pop();
          // this.statusVar = this.listOfWorkers[0].status;
        }));
  }

  calculateDuration(startTime: any, endTime: any) {
    startTime = new Date(startTime);
    endTime = new Date(endTime);
    let duration = new Date(0, 0, 0);
    var seconds = endTime.getTime() - startTime.getTime();
    duration.setMilliseconds(seconds);
    return duration;
  }

  openDialog(name) {
    let dialogRef = this.dialog.open(SteplogsComponent, {
      data: {
        Id: this.jobExecutionId,
        Name: name
      }
    });

    dialogRef.afterClosed().subscribe(result => {
      console.log(result);
    })
  }
}
