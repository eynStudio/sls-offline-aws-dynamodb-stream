"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const AWS = require("aws-sdk");
const DynamoDBStreamReadable_1 = require("./DynamoDBStreamReadable");
const stream_1 = require("stream");
const path_1 = require("path");
class SlsOfflineDynamodbStreamPlugin {
    constructor(serverless, options) {
        var _a;
        this.serverless = serverless;
        this.options = options;
        this.config = {};
        this.provider = "aws";
        this.commands = {};
        this.hooks = {
            "before:offline:start": () => this.startReadableStreams(),
        };
        this.config = Object.assign(Object.assign({}, this.config), (_a = serverless.service.custom) === null || _a === void 0 ? void 0 : _a.dynamodbStream);
    }
    startReadableStreams() {
        const { config: { endpoint, region, batchSize, pollForever = false, interval = 2000, } = {}, } = this;
        const offlineConfig = this.serverless.service.custom["serverless-offline"] || {};
        const fns = this.serverless.service.functions;
        let location = process.cwd();
        if (offlineConfig.location) {
            location = process.cwd() + "/" + offlineConfig.location;
        }
        else if (this.serverless.config.servicePath) {
            location = this.serverless.config.servicePath;
        }
        const streams = (this.config.streams || []).map(({ table, functions = [] }) => ({
            table,
            functions: functions.map((fnName) => fns[fnName]),
        }));
        streams.forEach(({ table, functions }) => {
            const dynamo = endpoint
                ? new AWS.DynamoDB({ region, endpoint })
                : new AWS.DynamoDB({ region });
            dynamo.describeTable({ TableName: table }, (err, tableDescription) => {
                var _a;
                if (err) {
                    throw err;
                }
                const streamArn = (_a = tableDescription === null || tableDescription === void 0 ? void 0 : tableDescription.Table) === null || _a === void 0 ? void 0 : _a.LatestStreamArn;
                if (streamArn) {
                    const ddbStream = endpoint
                        ? new AWS.DynamoDBStreams({
                            region,
                            endpoint,
                        })
                        : new AWS.DynamoDBStreams({ region });
                    const readable = new DynamoDBStreamReadable_1.DynamoDBStreamReadable(ddbStream, streamArn, pollForever, {
                        highWaterMark: batchSize,
                        interval,
                    });
                    const wriable = new stream_1.Writable({
                        write: (chunk = [], encoding, callback) => {
                            this.executeFunctions(chunk, location, functions).then(() => {
                                callback();
                            });
                        },
                        objectMode: true,
                    });
                    readable.on("error", (error) => {
                        this.log(`DynamoDBStreamReadable error... terminating stream... Error => ${error}`);
                        wriable.destroy(error);
                    });
                    readable.pipe(wriable).on("end", () => {
                        this.log(`stream for table [${table}] closed!`);
                    });
                }
            });
        });
    }
    log(msg) {
        this.serverless.cli.log(`sls-offline-aws-eventbridge ${msg}`);
    }
    executeFunctions(events = [], location, functions = []) {
        return Promise.all(functions.map(async (fn) => {
            const handler = this.createHandler(location, fn);
            return handler()(events, {}, () => { });
        }));
    }
    createHandler(location, fn) {
        return this.createJavascriptHandler(location, fn);
    }
    createJavascriptHandler(location, fn) {
        return () => {
            const handlerFnNameIndex = fn.handler.lastIndexOf(".");
            const handlerPath = fn.handler.substring(0, handlerFnNameIndex);
            const handlerFnName = fn.handler.substring(handlerFnNameIndex + 1);
            const fullHandlerPath = path_1.resolve(location, handlerPath);
            const handler = require(fullHandlerPath)[handlerFnName];
            return handler;
        };
    }
}
module.exports = SlsOfflineDynamodbStreamPlugin;
//# sourceMappingURL=index.js.map