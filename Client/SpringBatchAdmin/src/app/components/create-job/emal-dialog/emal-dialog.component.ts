import { Component, OnInit } from '@angular/core';
import { FormControl, FormGroup, Validators } from '@angular/forms';
import { MatDialogRef } from '@angular/material/dialog';

@Component({
  selector: 'app-emal-dialog',
  templateUrl: './emal-dialog.component.html',
  styleUrls: ['./emal-dialog.component.css']
})
export class EmalDialogComponent implements OnInit {
  mails : string[]= [];
  constructor(public dialogRef: MatDialogRef<EmalDialogComponent>) { }

  ngOnInit(): void {
  }

  emailForm = new FormGroup({
    email : new FormControl('', Validators.email)
  });

  addMails(){
    let mail = this.emailForm.get('email').value
    if(mail != '' && this.mails.indexOf(mail) == -1){
      this.mails.push(mail);
    }
    
  }
  onCloseConfirm() {
    this.dialogRef.close(this.mails);
  }

}
