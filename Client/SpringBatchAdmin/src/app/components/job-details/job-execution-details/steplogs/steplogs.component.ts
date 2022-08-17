import { Component, Inject, OnInit } from '@angular/core';
import { MAT_DIALOG_DATA } from '@angular/material/dialog';
import { ServiceService } from 'src/app/service.service';
import { map } from "rxjs/operators";

export interface DialogData {
  Id: String;
  Name: String;
}

@Component({
  selector: 'app-steplogs',
  templateUrl: './steplogs.component.html',
  styleUrls: ['./steplogs.component.css']
})
export class SteplogsComponent implements OnInit {

  logs = [];

  constructor(@Inject(MAT_DIALOG_DATA) public data: DialogData, private _service: ServiceService) { }

  ngOnInit(): void {
    this.getWorkerslogs(this.data.Id, this.data.Name).subscribe();
  }


  getWorkerslogs(id, name) {
    return this._service
      .getWorkerLog(id, name)
      .pipe(map(
        (users) => {
          this.logs = users;
        }));
  }


}
