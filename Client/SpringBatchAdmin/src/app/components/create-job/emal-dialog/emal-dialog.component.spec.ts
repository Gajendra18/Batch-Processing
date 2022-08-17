import { ComponentFixture, TestBed } from '@angular/core/testing';

import { EmalDialogComponent } from './emal-dialog.component';

describe('EmalDialogComponent', () => {
  let component: EmalDialogComponent;
  let fixture: ComponentFixture<EmalDialogComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ EmalDialogComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(EmalDialogComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
