import { Pipe, PipeTransform } from '@angular/core';
import { DomSanitizer } from '@angular/platform-browser';

@Pipe({
  name: 'status'
})
export class StatusPipe implements PipeTransform {

  constructor(private sanitized: DomSanitizer) { }

  transform(value: any, ...args: any[]): any {
    if (value == "Completed") {
      return this.sanitized.bypassSecurityTrustHtml(value?.replace(value, `<span style="background-color: #2ecc71;color:#fff;padding:4px 8px;border-radius:20px;">${value}</span>`));
    }
    else if (value == "Failed" || value == "Stopped") {
      return this.sanitized.bypassSecurityTrustHtml(value?.replace(value, `<span style="background-color: #e74c3c;color:#fff;padding:4px 8px;border-radius:20px;">${value}</span>`));
    }
    else if (value == "Stopping") {
      return this.sanitized.bypassSecurityTrustHtml(value?.replace(value, `<span style="background-color: #f39c12;color:#fff;padding:4px 8px;border-radius:20px;">${value}</span>`));
    }
    else if (value == "Started") {
      return this.sanitized.bypassSecurityTrustHtml(value?.replace(value, `<span style="background-color: #3498db;color:#fff;padding:4px 8px;border-radius:20px;">${value}</span>`));
    }
    else if (value == "Unknown") {
      return this.sanitized.bypassSecurityTrustHtml(value?.replace(value, `<span style="background-color: #f1c40f;color:#fff;padding:4px 8px;border-radius:20px;">In Queue</span>`));
    }
    else {
      return this.sanitized.bypassSecurityTrustHtml(value?.replace(value, `<span style="background-color: #f1c40f;color:#fff;padding:4px 8px;border-radius:20px;">${value}</span>`));

    }
  }







}
