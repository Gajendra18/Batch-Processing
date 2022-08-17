import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { CreateJobComponent } from './components/create-job/create-job.component';
import { DashboardComponent } from './components/dashboard/dashboard.component';
import { JobDetailsComponent } from './components/job-details/job-details.component';
import { JobExecutionDetailsComponent } from './components/job-details/job-execution-details/job-execution-details.component';
import { PageNotFoundComponent } from './components/page-not-found/page-not-found.component';
import { ScheduledJobsComponent } from './components/scheduled-jobs/scheduled-jobs.component';

const routes: Routes = [
  { path: '', component: DashboardComponent, pathMatch: 'full' },
  { path: 'dashboard', redirectTo: '' , pathMatch: 'full'},
  { path: 'schedule', component: ScheduledJobsComponent, pathMatch: 'full' },
  { path: 'create', component: CreateJobComponent, pathMatch: 'full' },
  { path: 'jobDetails/:jobID', component: JobDetailsComponent, pathMatch: 'full' },
  { path: 'jobDetails/:jobID/jobExecution/:jobExecutionId', component: JobExecutionDetailsComponent, pathMatch: 'full' },
  { path: 'scheduledJobs', component: ScheduledJobsComponent, pathMatch: 'full' },
  { path: '**', component:  PageNotFoundComponent, pathMatch: 'full'}


];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
