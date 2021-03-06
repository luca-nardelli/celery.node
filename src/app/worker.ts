import Base from "./base";
import { Message } from "../kombu/message";
import {EventMessage} from './event-message';

export default class Worker extends Base {
  handlers: object = {};
  activeTasks: Set<Promise<any>> = new Set();
  clock = 0;
  processed = 0;
  concurrency = 1;

  /**
   * register task handler on worker handlers
   * @method Worker#register
   * @param {String} name the name of task for dispatching.
   * @param {Function} handler the function for task handling
   *
   * @example
   * worker.register('tasks.add', (a, b) => a + b);
   * worker.start();
   */
  public register(name: string, handler: Function): void {
    if (!handler) {
      throw new Error("Undefined handler");
    }
    if (this.handlers[name]) {
      throw new Error("Already handler setted");
    }

    this.handlers[name] = function registHandler(...args: any[]): Promise<any> {
      try {
        return Promise.resolve(handler(...args));
      } catch (err) {
        return Promise.reject(err);
      }
    };
  }

  /**
   * start celery worker to run
   * @method Worker#start
   * @example
   * worker.register('tasks.add', (a, b) => a + b);
   * worker.start();
   */
  public start(): Promise<any> {
    console.info(`celery.node worker starting on queue ${this.conf.CELERY_QUEUE}`);
    console.info(`registered tasks: ${Object.keys(this.handlers)}`);
    return this.run().catch(err => console.error(err));
  }

  /**
   * @method Worker#run
   * @private
   *
   * @returns {Promise}
   */
  private run(): Promise<any> {
    return this.isReady().then(async () => {
      await this.sendWorkerOnline();
      setInterval(() => this.sendWorkerHeartbeat(),5000);
      this.processTasks();
    });
  }

  /**
   * @method Worker#processTasks
   * @private
   *
   * @returns function results
   */
  private processTasks(): Promise<any> {
    const consumer = this.getConsumer(this.conf.CELERY_QUEUE);
    return consumer();
  }

  /**
   * @method Worker#getConsumer
   * @private
   *
   * @param {String} queue queue name for task route
   */
  private getConsumer(queue: string): Function {
    const onMessage = this.createTaskHandler();

    return (): any => this.broker.subscribe(queue, onMessage,{concurrency: this.concurrency});
  }

  public createTaskHandler(): Function {
    // See https://github.com/celery/celery/blob/master/celery/worker/strategy.py
    const onTaskReceived = (message: Message): Promise<any> => {
      if (!message) {
        return Promise.resolve();
      }
      let payload = null;
      let taskName = message.headers["task"];
      if (!taskName) {
        // protocol v1
        payload = message.decode();
        taskName = payload["task"];
      }

      // strategy
      let body;
      let headers;
      if (payload == null && !("args" in message.decode())) {
        body = message.decode(); // message.body;
        headers = message.headers;
      } else {
        const args = payload["args"] || [];
        const kwargs = payload["kwargs"] || {};
        const embed = {
          callbacks: payload["callbacks"],
          errbacks: payload["errbacks"],
          chord: payload["chord"],
          chain: null
        };

        body = [args, kwargs, embed];
        headers = {
          lang: payload["lang"],
          task: payload["task"],
          id: payload["id"],
          rootId: payload["root_id"],
          parantId: payload["parentId"],
          group: payload["group"],
          meth: payload["meth"],
          shadow: payload["shadow"],
          eta: payload["eta"],
          expires: payload["expires"],
          retries: payload["retries"] || 0,
          timelimit: payload["timelimit"] || [null, null],
          kwargsrepr: payload["kwargsrepr"],
          origin: payload["origin"]
        };
      }

      // request
      const [args, kwargs /*, embed */] = body;
      const taskId = headers["id"];

      const handler = this.handlers[taskName];
      if (!handler) {
        throw new Error(`Missing process handler for task ${taskName}`);
      }

      const uuid = headers.id;
      this.sendTaskReceived({name: taskName,uuid,args,kwargs});
      // console.info(`celery.node Received task: ${taskName}[${taskId}]`);
      // console.info(`args: ${args}, kwargs: ${JSON.stringify(kwargs)}`)

      this.sendTaskStarted({uuid});
      const timeStart = process.hrtime();
      const taskPromise: Promise<any> = handler(args, kwargs)
        .then(result => {
        const diff = process.hrtime(timeStart);
        // console.info(
        //   `celery.node Task ${taskName}[${taskId}] succeeded in ${diff[0] +
        //     diff[1] / 1e9}s: ${result}`
        // );
        this.backend.storeResult(taskId, result, "SUCCESS",{replyTo: message.properties['replyTo'] || undefined});
        this.sendTaskSucceeded({runtime: diff[0] + diff[1] / 1e9, uuid, result});
        this.activeTasks.delete(taskPromise);
        this.processed++;
      })
        .catch((error : Error) => {
          console.error(`Task ${taskName}[${taskId}] failed ${error.message}`);
          this.backend.storeResult(taskId, {exc_type: 'error', exc_message: error.message},
            "FAILURE",{replyTo: message.properties['replyTo'] || undefined});
          this.sendTaskFailed({exception: 'error', traceback: error.stack});
          this.activeTasks.delete(taskPromise);
          this.processed++;
        })
      ;

      // record the executing task
      this.activeTasks.add(taskPromise);

      return taskPromise;
    };

    return onTaskReceived;
  }

  /**
   * @method Worker#whenCurrentJobsFinished
   *
   * @returns Promise that resolves when all jobs are finished
   */
  public async whenCurrentJobsFinished(): Promise<any[]> {
    return Promise.all(Array.from(this.activeTasks));
  }

  /**
   * @method Worker#stop
   *
   * @todo implement here
   */
  // eslint-disable-next-line class-methods-use-this
  public stop(): any {
    throw new Error("not implemented yet");
  }

  private async sendWorkerOnline(): Promise<void>{
    const msg = new EventMessage(this.clock++,{
      type: 'worker-online',
    },{},{
      exchange: 'celeryev',
    });
    return this.broker.publish(msg.body,msg.properties.exchange,msg.properties.routing_key,msg.headers,msg.properties);
  }

  private async sendWorkerHeartbeat(): Promise<void>{
    const msg = new EventMessage(this.clock++,{
      type: 'worker-heartbeat',
      active: this.activeTasks.size,
      processed: this.processed,
    },{},{
      exchange: 'celeryev',
    });
    return this.broker.publish(msg.body,msg.properties.exchange,msg.properties.routing_key,msg.headers,msg.properties);
  }

  private async sendTaskSucceeded(data: {runtime: number, uuid: string, result: any}): Promise<void>{
    const msg = new EventMessage(this.clock++,{
      type: 'task-succeeded',
      ...data
    },{},{
      exchange: 'celeryev',
    });
    return this.broker.publish(msg.body,msg.properties.exchange,msg.properties.routing_key,msg.headers,msg.properties);
  }

  private async sendTaskFailed(data: {exception: string, traceback: string}): Promise<void>{
    const msg = new EventMessage(this.clock++,{
      type: 'task-failed',
      ...data,
    },{},{
      exchange: 'celeryev',
    });
    return this.broker.publish(msg.body,msg.properties.exchange,msg.properties.routing_key,msg.headers,msg.properties);
  }

  private async sendTaskReceived(data: {name: string, uuid: string, args?: any[], kwargs?: object}): Promise<void>{
    const msg = new EventMessage(this.clock++,{
      type: 'task-received',
      ...data,
    },{},{
      exchange: 'celeryev',
    });
    if(msg.body.kwargs && typeof msg.body.kwargs === 'object') {
      msg.body.kwargs = JSON.stringify(msg.body.kwargs)
    }
    return this.broker.publish(msg.body,msg.properties.exchange,msg.properties.routing_key,msg.headers,msg.properties);
  }

  private async sendTaskStarted(data: {uuid: string}): Promise<void>{
    const msg = new EventMessage(this.clock++,{
      type: 'task-started',
      ...data,
    },{},{
      exchange: 'celeryev',
    });
    return this.broker.publish(msg.body,msg.properties.exchange,msg.properties.routing_key,msg.headers,msg.properties);
  }
}
