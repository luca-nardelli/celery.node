import * as amqplib from "amqplib";
import {CeleryBackend, StoreResultOpts} from ".";
import {EventEmitter} from 'events';

// Inspired by https://github.com/Igor-lkm/node-rabbitmq-rpc-direct-reply-to/blob/master/client.js
export default class RPCBackend implements CeleryBackend {

  channel: Promise<amqplib.Channel>;
  _replyQueueSubscription: Promise<amqplib.Replies.Consume>;
  private eventEmitter: EventEmitter = new EventEmitter();

  private resultMap = new Map<string, Promise<object>>();

  constructor() {
    this.eventEmitter.setMaxListeners(0);
  }

  /**
   * @method AMQPBackend#isReady
   * @returns {Promise} promises that continues if amqp connected.
   */
  public async isReady(): Promise<void> {
    const ch = await this.channel; // Need to use the same channel we use to publish
    if (this._replyQueueSubscription === undefined) {
      // Start consuming to trigger creation of the pseudo-queue
      this._replyQueueSubscription = ch.consume('amq.rabbitmq.reply-to', msg => {
        if (msg.properties.contentType !== "application/json") {
          throw new Error(
            `unsupported content type ${msg.properties.contentType}`
          );
        }

        if (msg.properties.contentEncoding !== "utf-8") {
          throw new Error(
            `unsupported content encoding ${msg.properties.contentEncoding}`
          );
        }
        const body = msg.content.toString("utf-8");
        const parsed = JSON.parse(body);
        if (!parsed['task_id']) {
          console.warn('Received RPC answer without a task id. Ignoring');
          return;
        }
        this.eventEmitter.emit(parsed['task_id'], parsed);
        return;
      }, {noAck: true});
    }
    await this._replyQueueSubscription;
    return;
  }

  expectReply(taskId: string): void {
    this.resultMap.set(taskId, new Promise(resolve => {
      this.eventEmitter.once(taskId, resolve);
    }));
  }

  /**
   * @method AMQPBackend#disconnect
   * @returns {Promise}
   */
  public disconnect(): Promise<void> {
    // Do nothing, as we are dependent on the broker here
    return;
  }

  /**
   * store result method on backend
   * @method AMQPBackend#storeResult
   * @param {String} taskId
   * @param {any} result result of task. i.e the return value of task handler
   * @param {String} state
   * @param opts
   * @returns {Promise}
   */
  public storeResult(
    taskId: string,
    result: any,
    state: string,
    opts?: StoreResultOpts,
  ): Promise<boolean> {
    if (!opts.replyTo) {
      throw new Error('Cannot reply without a reply_to header');
    }
    return this.channel
      .then(ch =>
        ch.publish(
          "",
          opts.replyTo,
          Buffer.from(
            JSON.stringify({
              status: state,
              result,
              traceback: null,
              children: [],
              task_id: taskId,
              date_done: new Date().toISOString()
            })
          ),
          {
            contentType: "application/json",
            contentEncoding: "utf-8"
          }
        )
      );
  }

  /**
   * Get result data from backend
   * @method AMQPBackend#getTaskMeta
   * @param {String} taskId
   * @returns {Promise}
   */
  public getTaskMeta(taskId: string): Promise<any> {
    return this.resultMap.get(taskId);
  }
}
