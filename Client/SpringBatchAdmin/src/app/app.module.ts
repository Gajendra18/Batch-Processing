import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { ReactiveFormsModule } from '@angular/forms';
import { AppRoutingModule } from './app-routing.module';
import { HttpClientModule } from '@angular/common/http';
import { AppComponent } from './app.component';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { MaterialModule } from './material/material.module';
import { ServiceService } from './service.service';
import { StatusPipe } from './pipes/status.pipe';
import { DashboardComponent } from './components/dashboard/dashboard.component';
import { ScheduledJobsComponent } from './components/scheduled-jobs/scheduled-jobs.component';
import { CreateJobComponent } from './components/create-job/create-job.component';
import { FormsModule } from '@angular/forms';
import { JobDetailsComponent } from './components/job-details/job-details.component';
import { EmalDialogComponent } from './components/create-job/emal-dialog/emal-dialog.component';
import { JobExecutionDetailsComponent } from './components/job-details/job-execution-details/job-execution-details.component';
import { SteplogsComponent } from './components/job-details/job-execution-details/steplogs/steplogs.component';
import { SplitNamePipe } from './pipes/split-name.pipe';
import { StopDialogComponent } from './components/stop-dialog/stop-dialog.component';
import { InjectableRxStompConfig, RxStompService, rxStompServiceFactory } from '@stomp/ng2-stompjs';
import { myRxStompConfig } from './rx-stomp.config';
import { Globals } from './globals';
import { PageNotFoundComponent } from './components/page-not-found/page-not-found.component';

@NgModule({
  declarations: [
    AppComponent,
    StatusPipe,
    DashboardComponent,
    ScheduledJobsComponent,
    CreateJobComponent,
    JobDetailsComponent,
    EmalDialogComponent,
    JobExecutionDetailsComponent,
    SteplogsComponent,
    SplitNamePipe,
    StopDialogComponent,
    PageNotFoundComponent
  ],
  entryComponents: [EmalDialogComponent],
  imports: [
    BrowserModule,
    AppRoutingModule,
    BrowserAnimationsModule,
    MaterialModule,
    HttpClientModule,
    FormsModule,
    ReactiveFormsModule
  ],
  providers: [ServiceService, Globals,
    {
      provide: InjectableRxStompConfig,
      useValue: myRxStompConfig
    },
    {
      provide: RxStompService,
      useFactory: rxStompServiceFactory,
      deps: [InjectableRxStompConfig]
    }],
  bootstrap: [AppComponent]
})
export class AppModule { }
