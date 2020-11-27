import * as os from 'os';
import * as process from 'process';

interface EventMessageProperties {
  routing_key?: string;
  exchange?: string;
  content_type?: string;
  content_encoding?: string;
  delivery_mode?: number;
  [x:string]: any;
}

export class EventMessage {
  body: object | any;
  headers: object;
  properties: EventMessageProperties;

  constructor(clock: number, body: object, headers: object = {}, properties: EventMessageProperties = {}) {
    const offset = (new Date()).getTimezoneOffset() / 60;
    this.body = {
      ...body,
      hostname: os.hostname(),
      clock: clock,
      timestamp: (new Date()).getTime()/1000.0,
      utcoffset: offset,
      pid: process.pid
    };
    this.headers = {
      ...headers,
      hostname: os.hostname(),
    };
    this.properties = properties;
  }
}
