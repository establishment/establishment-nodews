const fs = require("fs");
const path = require("path");

const {GCScheduler} = require("establishment-node-core");
const {Glue, RPCServer, Util} = require("establishment-node-service-core");

const WebsocketServer = require("./WebsocketServer.js6.js");
const UserConnection = require("./UserConnection.js6.js");
const DefaultConfig = require("./DefaultConfig.js6.js");

module.exports.run = (params) => {
    let config = null;
    if (params) {
        if (params.hasOwnProperty("config") && params.config != null) {
            config = params.config;
        } else if (params.hasOwnProperty("configFilePath") && params.configFilePath != null) {
            config = JSON.parse(fs.readFileSync(params.configFilePath, "utf8"));
        }
    }

    if (!config) {
        config = DefaultConfig();
    }

    Util.setMockMachineId(config.machineId.mockId);
    Util.setMachineIdScript(config.machineId.script);
    Glue.initLogger(config.logging);
    Glue.initRegistryKeeper(config.registryKeeper);
    Glue.initService(config.service);

    GCScheduler.configure(config.gc);
    GCScheduler.setLogger(Glue.logger);
    GCScheduler.start();

    let rpcServer = new RPCServer(config.rpcServer);
    rpcServer.start();

    rpcServer.on("stop", (params, rpcCallback) => {
        Glue.stop(params, rpcCallback);
    });

    rpcServer.on("refresh", (params, rpcCallback) => {
        Glue.logger.info("Refresh command issued: executing now!");
        if (!params.hasOwnProperty("batchSize")) {
            let error = "RPC Refresh error: params does not contain 'batchSize'";
            Glue.logger.error(error);
            callback(error);
        }
        if (!params.hasOwnProperty("batchDelay")) {
            let error = "RPC Refresh error: params does not contain 'batchDelay'";
            Glue.logger.error(error);
            callback(error);
        }
        if (!params.hasOwnProperty("delay")) {
            let error = "RPC Refresh error: params does not contain 'delay'";
            Glue.logger.error(error);
            callback(error);
        }

        let userConnections = UserConnection.getAll();
        let batch = [];
        let batches = [];
        for (let userConnection of userConnections) {
            batch.push(userConnection);
            if (batch.length === params.batchSize) {
                batches.push(batch);
                batch = [];
            }
        }
        if (batch.length !== 0) {
            batches.push(batch);
        }

        let batchIndex = 0;
        let currentBatch = 0;

        let sendBatchRefresh = (callback) => {
            if (batchIndex == batches[currentBatch].length) {
                callback();
            } else {
                batches[currentBatch][batchIndex].sendCommand("refresh");
                ++batchIndex;
                setTimeout(() => {
                    sendBatchRefresh(callback)
                }, params.delay);
            }
        };

        let sendRefresh = () => {
            if (currentBatch == batches.length) {
                rpcCallback("RPC Refresh: finish!");
            } else {
                batchIndex = 0;
                sendBatchRefresh(() => {
                    ++currentBatch;
                    setTimeout(sendRefresh, params.batchDelay);
                });
            }
        };

        sendRefresh();
    });

    let websocketServer = new WebsocketServer(config.server);
    websocketServer.start();
};