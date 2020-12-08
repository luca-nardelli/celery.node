import * as amqplib from "amqplib";
import {CeleryBroker, SubscribeOpts} from ".";
import { Message } from "../message";
import RPCBackend from '../../backends/rpc';

class AMQPMessage extends Message {
  constructor(payload: amqplib.ConsumeMessage) {
    super(
      payload.content,
      payload.properties.contentType,
      payload.properties.contentEncoding,
      payload.properties,
      payload.properties.headers
    );
  }
}

export default class AMQPBroker implements CeleryBroker {
  connect: Promise<amqplib.Connection>;
  channel: Promise<amqplib.Channel>;
  queue: string;
  rpcBackend: RPCBackend; // Reference used for replyTo Functionality

  /**
   * AMQP broker class
   * @constructor AMQPBroker
   * @param {string} url the connection string of amqp
   * @param {object} opts the options object for amqp connect of amqplib
   * @param {string} queue optional. the queue to connect to.
   */
  constructor(url: string, opts: object, queue = "celery") {
    this.queue = queue;
    this.connect = amqplib.connect(url, opts);
    this.channel = this.connect.then(conn => conn.createChannel());
  }

  /**
   * @method AMQPBroker#isReady
   * @returns {Promise} promises that continues if amqp connected.
   */
  public isReady(): Promise<amqplib.Channel> {
    return new Promise(resolve => {
      this.channel.then(ch => {
        Promise.all([
          ch.assertExchange("default", "direct", {
            durable: true,
            autoDelete: true,
            internal: false,
            // nowait: false,
            arguments: null
          }),
          ch.assertQueue(this.queue, {
            durable: true,
            autoDelete: false,
            exclusive: false,
            // nowait: false,
            arguments: null
          })
        ]).then(() => resolve());
      });
    });
  }

  /**
   * @method AMQPBroker#disconnect
   * @returns {Promise} promises that continues if amqp disconnected.
   */
  public disconnect(): Promise<void> {
    return this.connect.then(conn => conn.close());
  }

  /**
   * @method AMQPBroker#publish
   *
   * @returns {Promise}
   */
  public async publish(
    body: object | [Array<any>, object, object],
    exchange: string,
    routingKey: string,
    headers: object,
    properties: object
  ): Promise<boolean> {
    const messageBody = JSON.stringify(body);
    const contentType = "application/json";
    const contentEncoding = "utf-8";

    // Create queue only if a routing key is specified
    // TODO Maybe we can remove this here
    if(routingKey){
      await this.channel.then(ch =>
        ch.assertQueue(routingKey, {
          durable: true,
          autoDelete: false,
          exclusive: false,
          // nowait: false,
          arguments: null
        })
      );
    }

    return this.channel
      .then(ch =>
        ch.publish(exchange, routingKey, Buffer.from(messageBody), {
          contentType,
          contentEncoding,
          headers,
          ...properties
        })
      );
  }

  /**
   * @method AMQPBroker#subscribe
   * @param {String} queue
   * @param {Function} callback
   * @returns {Promise}
   */
  public subscribe(
    queue: string,
    callback: (message: Message) => void,
    opts?: SubscribeOpts,
  ): Promise<amqplib.Replies.Consume> {

    // this.channel.then(async(ch) => {
    //   await ch.assertQueue(`${queue}.pidbox`,{
    //     autoDelete: true,
    //     expires: 10000,
    //     messageTtl: 300000,
    //   })
    //   await ch.bindQueue(`${queue}.pidbox`,'celery.pidbox','');
    //   return ch.consume(`${queue}.pidbox`, rawMsg => {
    //     ch.ack(rawMsg);
    //     // now supports only application/json of content-type
    //     if (rawMsg.properties.contentType !== "application/json") {
    //       throw new Error(
    //         `unsupported content type ${rawMsg.properties.contentType}`
    //       );
    //     }
    //
    //     // now supports only utf-9 of content-encoding
    //     if (rawMsg.properties.contentEncoding !== "utf-8") {
    //       throw new Error(
    //         `unsupported content encoding ${rawMsg.properties.contentEncoding}`
    //       );
    //     }
    //     const parsed = new AMQPMessage(rawMsg);
    //     console.log(parsed.decode());
    //   })
    // })
    return this.channel
      .then(ch =>
        ch
          .assertQueue(queue, {
            durable: true,
            autoDelete: false,
            exclusive: false,
            // nowait: false,
            arguments: null
          })
          .then(() => Promise.resolve(ch))
      )
      .then(ch => {
          ch.prefetch(opts.concurrency || 1); // Prefetch messages
          return ch.consume(queue, async rawMsg => {
            // now supports only application/json of content-type
            if (rawMsg.properties.contentType !== "application/json") {
              throw new Error(
                `unsupported content type ${rawMsg.properties.contentType}`
              );
            }

            // now supports only utf-9 of content-encoding
            if (rawMsg.properties.contentEncoding !== "utf-8") {
              throw new Error(
                `unsupported content encoding ${rawMsg.properties.contentEncoding}`
              );
            }

            try {
              await callback(new AMQPMessage(rawMsg));
            } catch (e) {
              console.error(e)
              throw e;
            }
            ch.ack(rawMsg); // Ack here to prevent the worker from fetching too many messages
          })
        }
      );
  }
}
