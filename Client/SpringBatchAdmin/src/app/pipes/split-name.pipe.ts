import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'splitName'
})
export class SplitNamePipe implements PipeTransform {

  transform(value: String, ...args: unknown[]): unknown {
    return value.split(":")[0];
  }

}
