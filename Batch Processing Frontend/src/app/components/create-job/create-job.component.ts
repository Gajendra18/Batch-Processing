import { Component, OnInit } from '@angular/core';
import { FormControl, FormGroup, Validators } from '@angular/forms';
import { MatDialog } from '@angular/material/dialog';
import { jobParameters } from '../../Entities/jobParameters';
import { jobScheduler } from '../../Entities/jobScheduler';
import { ServiceService } from '../../service.service';
import { EmalDialogComponent } from './emal-dialog/emal-dialog.component';
import { MatSnackBar } from '@angular/material/snack-bar';
import { Router } from '@angular/router';

@Component({
  selector: 'app-create-job',
  templateUrl: './create-job.component.html',
  styleUrls: ['./create-job.component.css']
})
export class CreateJobComponent implements OnInit {
  srcResult;
  mails: string[] = [];
  email = false;
  timeValue = '0';
  mailsList = "";
  files = [];
  displayEmail = false;
  constructor(public dialog: MatDialog, private _service: ServiceService, private _snackBar: MatSnackBar, private router: Router) { }
  jobParams: jobParameters;
  jobSchedule: jobScheduler;
  validate;
  loading = false;
  checked = false;

  createForm = new FormGroup({
    jobName: new FormControl('', Validators.required),
    jobDescription: new FormControl('', Validators.required),
    input: new FormControl(this.files, Validators.required),
    collectionName : new FormControl('', Validators.required),
    size: new FormControl(1, Validators.required),
    time: new FormControl("0"),
    jobGroup: new FormControl(' ', Validators.required),
    cron: new FormControl(' ', Validators.required),
    check: new FormControl(this.checked)
  });

  emailForm = new FormGroup({
    email: new FormControl('', Validators.email)
  });

  ngOnInit(): void {
  }

  openDialog() {
    let dialogRef = this.dialog.open(EmalDialogComponent, { disableClose: true });
    dialogRef.afterClosed().subscribe(result => {
      this.mails = result;
      

    })
  }

  input() {
    
    if (this.timeValue == "1") {
      this.createForm.get('jobGroup').setValue('');
      this.createForm.get('cron').setValue('');
    }
    else if (this.timeValue == "0") {
      this.createForm.get('jobGroup').setValue(' ');
      this.createForm.get('cron').setValue(' ');
    }
  }

  submitJob() {
    if (this.mails.length > 0) {
      for (let i = 0; i < this.mails.length; i++) {
        if (i == 0) {
          this.mailsList = this.mails[0];
        }
        else {
          this.mailsList += "," + this.mails[i];
        }

      }

    }

    if (this.timeValue == "0") {
      this.jobParams = new jobParameters();
      this.jobParams.inputSource = this.createForm.get('input').value;
      

      this.jobParams.jobName = this.createForm.get('jobName').value.trim();
      this.jobParams.jobDescription = this.createForm.get('jobDescription').value.trim();
      this.jobParams.partitionSize = this.createForm.get('size').value;
      this.jobParams.mailRecipients = this.mailsList;
      this.jobParams.collectionName = this.createForm.get('collectionName').value.trim();
      this.jobParams.restart = this.createForm.get('check').value;
      this.loading = true;
      this._service.startJob(this.files, this.jobParams).subscribe(data => {
        if (data != null) {
          this.loading = false;
          let snakbarRef = this._snackBar.open('Job Created', 'View status', {
            duration: 5000
          });
          snakbarRef.onAction().subscribe(() => {
            this.router.navigate(['jobDetails/'+ data]);
          });
        }
      }, (error => {
        this.loading = false;
        let msg: string = error.error.errorMessage;
        if (msg.includes("JOB_ALREADY")) {
          this.createForm.get('jobName').setErrors(
            { jobNameEror: true })
        }else if (msg.includes("TABLE_ALREADY")) {
          this.createForm.get('collectionName').setErrors(
            { collectionNameEror: true })
        }else if (msg.includes("EMPTY_FILE")) {
          this.createForm.get('input').setErrors(
            { emptyFile: true })
        }else if (msg.includes("MAX_UPLOAD_SIZE_EXCEEDED")) {
          this.createForm.get('input').setErrors(
            { maxUploadSize: true })
        }
        else{
          this.loading = false;
          let snakbarRef = this._snackBar.open('Please try again later!', 'View DashBoard', {
            duration: 5000
          });
          snakbarRef.onAction().subscribe(() => {
            this.router.navigate(['/']);
          });
        }
      })
      );


      
    } else if (this.timeValue == "1") {
      this.jobSchedule = new jobScheduler();
      this.jobSchedule.inputSource = this.createForm.get('input').value;
      this.jobSchedule.jobName = this.createForm.get('jobName').value.trim();
      this.jobSchedule.jobGroup = this.createForm.get('jobGroup').value.trim();
      this.jobSchedule.jobDescription = this.createForm.get('jobDescription').value.trim();
      this.jobSchedule.partitionSize = this.createForm.get('size').value;
      this.jobSchedule.cronExpression = this.createForm.get('cron').value.trim();
      this.jobSchedule.mailRecipients = this.mailsList;
      this.jobSchedule.collectionName = this.createForm.get('collectionName').value.trim();
      this.jobSchedule.restart = this.createForm.get('check').value;
      this.loading = true;
      this._service.scheduleJob(this.files, this.jobSchedule).subscribe(data => {
        if (data == true) {
          this.loading = false;
          let snakbarRef = this._snackBar.open('Job Scheduled Successfully!', 'View DashBoard', {
            duration: 5000
          });
          snakbarRef.onAction().subscribe(() => {
            this.router.navigate(['/scheduledJobs']);
          });
        }
      }, (error => {
        this.loading = false;
        let msg: string = error.error.errorMessage;
        if (msg.includes("JOB_ALREADY")) {
          this.createForm.get('jobName').setErrors(
            { jobNameEror: true })
        }else if (msg.includes("TABLE_ALREADY")) {
          this.createForm.get('collectionName').setErrors(
            { collectionNameEror: true })
        }else if (msg.includes("INVALID_CRON_EXPRESSION")) {
          this.createForm.get('cron').setErrors(
            { cronEror: true })
        }
        else{
          this.loading = false;
          let snakbarRef = this._snackBar.open('Please try again later!', 'View DashBoard', {
            duration: 5000
          });
          snakbarRef.onAction().subscribe(() => {
            this.router.navigate(['/']);
          });
        }
      })
    );
    }
  }


  upload(event) {
    this.files = event.target.files;
  }

}
