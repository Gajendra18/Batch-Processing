import { Component, OnInit } from '@angular/core';
import { MatDialogRef } from '@angular/material/dialog';


@Component({
  selector: 'app-stop-dialog',
  templateUrl: './stop-dialog.component.html',
  styleUrls: ['./stop-dialog.component.css']
})
export class StopDialogComponent implements OnInit {

  constructor(public dialogRef: MatDialogRef<StopDialogComponent>) { }

  ngOnInit(): void {
  }

}
