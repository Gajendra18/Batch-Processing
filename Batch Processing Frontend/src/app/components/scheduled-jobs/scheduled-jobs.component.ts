import { Component, OnInit, ViewChild } from '@angular/core';
import { ServiceService } from '../../service.service';
import { MatTableDataSource } from '@angular/material/table';
import { map } from "rxjs/operators";
import { MatPaginator } from '@angular/material/paginator';
import { MatSnackBar } from '@angular/material/snack-bar';
import { MatDialog } from '@angular/material/dialog';
import { StopDialogComponent } from '../stop-dialog/stop-dialog.component';


@Component({
  selector: 'app-scheduled-jobs',
  templateUrl: './scheduled-jobs.component.html',
  styleUrls: ['./scheduled-jobs.component.css']
})
export class ScheduledJobsComponent implements OnInit {
  listOfJobs = [];
  dataSource;
  displayedColumns;
  loading = true;
  @ViewChild(MatPaginator) paginator: MatPaginator;

  constructor(private _service: ServiceService, private _snackBar: MatSnackBar, public dialog: MatDialog) { }

  ngOnInit(): void {
    this.getJobs().subscribe(_ => {
      this.dataSource = new MatTableDataSource(this.listOfJobs);
      this.loading = false;
      this.displayedColumns = ['jobName', 'jobGroup', 'cronExpression', 'actions'];
      this.dataSource.paginator = this.paginator;
    },
      (error) => {
        this.loading = false;
      });
  }

  getJobs() {
    return this._service
      .getScheduled()
      .pipe(map(
        (users) => {
          this.listOfJobs = users;
          

        }));
  }

  stopSchedule(jobName, jobGroup) {
    let dialogRef = this.dialog.open(StopDialogComponent);
    dialogRef.afterClosed().subscribe(result => {
      if (result == "true") {
        this._service.unscheduleJob(jobName, jobGroup).subscribe(status => {
          if (status == true) {
            this.ngOnInit();
            this._snackBar.open('Job Unscheduled', 'Dismiss', {
              duration: 5000
            });
          } else {
            this._snackBar.open('Please Try Again Later!', 'Dismiss', {
              duration: 5000
            });
          }
        })
      }
    })
  }
}
