import * as AWS from "aws-sdk";
import { DynamoDBStreamReadable } from "./DynamoDBStreamReadable";
import { Writable } from "stream";
import { resolve } from "path";

class SlsOfflineDynamodbStreamPlugin {
  config: any = {};

  provider = "aws";
  commands = {};
  hooks = {
    "before:offline:start": () => this.startReadableStreams(),
  };

  constructor(private serverless: any, private options: any) {
    this.config = {
      ...this.config,
      ...serverless.service.custom?.dynamodbStream,
    };
  }

  startReadableStreams() {
    const {
      config: {
        endpoint,
        region,
        batchSize,
        pollForever = false,
        interval = 2000,
      } = {},
    } = this;
    const offlineConfig =
      this.serverless.service.custom["serverless-offline"] || {};
    const fns = this.serverless.service.functions;

    let location = process.cwd();
    if (offlineConfig.location) {
      location = process.cwd() + "/" + offlineConfig.location;
    } else if (this.serverless.config.servicePath) {
      location = this.serverless.config.servicePath;
    }

    const streams = (this.config.streams || []).map(
      ({ table, functions = [] }) => ({
        table,
        functions: functions.map((fnName) => fns[fnName]),
      })
    );

    streams.forEach(({ table, functions }) => {
      const dynamo = endpoint
        ? new AWS.DynamoDB({ region, endpoint })
        : new AWS.DynamoDB({ region });
      dynamo.describeTable({ TableName: table }, (err, tableDescription) => {
        if (err) {
          throw err;
        }
        const streamArn = tableDescription?.Table?.LatestStreamArn;

        if (streamArn) {
          const ddbStream = endpoint
            ? new AWS.DynamoDBStreams({
                region,
                endpoint,
              })
            : new AWS.DynamoDBStreams({ region });

          const readable = new DynamoDBStreamReadable(
            ddbStream,
            streamArn,
            pollForever,
            {
              highWaterMark: batchSize,
              interval,
            }
          );

          const wriable = new Writable({
            write: (chunk = [], encoding, callback) => {
              this.executeFunctions(chunk, location, functions).then(() => {
                callback();
              });
            },
            objectMode: true,
          });

          readable.on("error", (error) => {
            this.log(
              `DynamoDBStreamReadable error... terminating stream... Error => ${error}`
            );
            wriable.destroy(error);
          });

          readable.pipe(wriable).on("end", () => {
            this.log(`stream for table [${table}] closed!`);
          });
        }
      });
    });
  }
  log(msg: string) {
    this.serverless.cli.log(`sls-offline-aws-eventbridge ${msg}`);
  }

  executeFunctions(events = [], location, functions = []) {
    return Promise.all(
      functions.map(async (fn) => {
        const handler = this.createHandler(location, fn);
        return handler()(events, {}, () => {});
      })
    );
  }

  createHandler(location: string, fn: any) {
    return this.createJavascriptHandler(location, fn);
  }

  createJavascriptHandler(location: string, fn: any) {
    return () => {
      const handlerFnNameIndex = fn.handler.lastIndexOf(".");
      const handlerPath = fn.handler.substring(0, handlerFnNameIndex);
      const handlerFnName = fn.handler.substring(handlerFnNameIndex + 1);
      const fullHandlerPath = resolve(location, handlerPath);
      const handler = require(fullHandlerPath)[handlerFnName];
      return handler;
    };
  }
}

module.exports = SlsOfflineDynamodbStreamPlugin;
