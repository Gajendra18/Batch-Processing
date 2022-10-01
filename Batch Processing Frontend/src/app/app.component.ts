import { Component, OnInit } from '@angular/core';
import { Subscription } from "rxjs";
import { Message } from '@stomp/stompjs';
import { RxStompService } from "@stomp/ng2-stompjs";
import { MatSnackBar } from '@angular/material/snack-bar';
import { Router } from '@angular/router';





@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent implements OnInit {
  title = 'SpringBatchAdmin';
  topicSubscription: Subscription;



  constructor(private rxStompService: RxStompService, private _snackBar: MatSnackBar, private router: Router) { }
  ngOnInit(): void {
    this.topicSubscription = this.rxStompService.watch("/topic/public").subscribe((message: Message) => {
      let jsonMessage = JSON.parse(message.body);
      if (jsonMessage['status'] == "started" || jsonMessage['status'] == "completed") {
        let snakbarRef = this._snackBar.open('Job execution ' + jsonMessage['status'] + '!', 'View status', {
          duration: 5000
        });
        snakbarRef.onAction().subscribe(() => {
          this.router.navigate(['jobDetails/'+jsonMessage['jobInstanceId']]);
        });
      }
    });
  }
}