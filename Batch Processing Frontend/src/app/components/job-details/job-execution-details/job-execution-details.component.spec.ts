import { ComponentFixture, TestBed } from '@angular/core/testing';

import { JobExecutionDetailsComponent } from './job-execution-details.component';

describe('JobExecutionDetailsComponent', () => {
  let component: JobExecutionDetailsComponent;
  let fixture: ComponentFixture<JobExecutionDetailsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ JobExecutionDetailsComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(JobExecutionDetailsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
