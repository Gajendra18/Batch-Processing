import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SteplogsComponent } from './steplogs.component';

describe('SteplogsComponent', () => {
  let component: SteplogsComponent;
  let fixture: ComponentFixture<SteplogsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ SteplogsComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(SteplogsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
