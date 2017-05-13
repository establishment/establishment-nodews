const {RedisConnectionPool, Queue} = require("establishment-node-core");
const {Glue} = require("establishment-node-service-core");

class MetadataBridge {
    constructor(config, uidFactory) {
        this.config = config;
        this.redisPublisher = RedisConnectionPool.getSharedConnection(config.redis.address);
        this.redisSubscriber = RedisConnectionPool.getConnection(config.redis.address);
        this.taskQueue = new Queue();

        this.redisPublisher.on("error", (error) => {
            Glue.logger.error("Establishment::MetadataBridge Redis publisher connection: " + error);
        });

        this.redisSubscriber.on("error", (error) => {
            Glue.logger.error("Establishment::MetadataBridge Redis subscriber connection: " + error);
        });
        
        this.redisSubscriber.on("message", (channel, message) => {
            this.processMessage(message);
        });

        this.uid = null;

        uidFactory.requestUID((uid) => {
            this.assignUID(uid);
        });

        this.redisSubscriber.subscribe(config.redis.inputStream);

        this.userConnection = new Map();

        this.syncWithState();

        this.keepAliveTimeout = null;
        this.resetKeepAlive();
    }

    assignUID(uid) {
        this.uid = uid;
        this.processTaskQueue(this.taskQueue);
    }

    processTaskQueue(taskQueue) {
        while (!taskQueue.empty()) {
            let request = taskQueue.pop();
            if (request["func"] == "send") {
                this.sendMessage(request["param0"]);
            } else {
                Glue.logger.critical("Establishment::MetadataBRidge: undefined request-func: " + request["func"]);
            }
        }
    }

    sendMessage(message) {
        if (this.uid != null) {
            message.id = this.uid;
            this.redisPublisher.publish(this.config.redis.outputStream, JSON.stringify(message));
        } else {
            let task = {
                func: "send",
                param0: message
            };
            this.taskQueue.push(task);
        }

        this.resetKeepAlive();
    }

    resetKeepAlive() {
        if (this.keepAliveTimeout != null) {
            clearTimeout(this.keepAliveTimeout);
            this.keepAliveTimeout = null;
        }
        if (this.config.redis.hasOwnProperty("keepAliveTime")) {
            this.keepAliveTimeout = setTimeout(() => {
                this.keepAlive(this.config.redis.keepAliveTime);
            }, this.config.redis.keepAliveTime);
        }
    }

    keepAlive(expireTime) {
        let message = {
            type: "keepAlive",
            timeout: expireTime
        };

        this.sendMessage(message);
    }

    processMessage(message) {
        message = this.parseMessage(message);
        if (message == null) {
            return;
        }

        if (message.type == "requestSyncAll") {
            this.syncWithState();
        } else if (message.type == "checkAliveAll") {
            this.checkAlive();
        } else if (message.type == "requestSync") {
            if (!message.hasOwnProperty("id")) {
                Glue.logger.critical("Establishment::MetadataBridge: requestSync message should contain id!");
                return;
            }
            if (message.id == this.uid) {
                this.syncWithState();
            }
        } else if (message.type == "checkAlive") {
            if (!message.hasOwnProperty("id")) {
                Glue.logger.critical("Establishment::MetadataBridge: checkAlive message should contain id!");
                return;
            }
            if (message.id == this.uid) {
                this.checkAlive();
            }
        } else {
            Glue.logger.critical("Establishment::MetadataBridge: bad message \"" + JSON.stringify(message) +
                                 "\": invalid type!");
        }
    }

    checkAlive() {
        this.keepAlive(this.config.redis.keepAliveTime);
    }

    parseMessage(message) {
        try {
            message = JSON.parse(message);
        } catch (error) {
            Glue.logger.critical("Establishment::MetadataBridge: bad message \"" + JSON.stringify(message) + "\": " +
                                 error);
            return null;
        }
        if (!message.hasOwnProperty("type")) {
            Glue.logger.critical("Establishment::MetadataBridge: bad message \"" + JSON.stringify(message) +
                                 "\": no type property");
            return null;
        }
        return message;
    }

    syncWithState() {
        let state = {
            type: "syncWithState",
            commands: this.getStateAsCommands()
        };
        this.sendMessage(state);
    }

    getStateAsCommands() {
        let commands = [];
        for (let [connectionId, userConnection] of this.userConnection) {
            let message = {
                type: "userConnectionNewEvent",
                connectionId: connectionId
            };
            commands.push(message);

            if (userConnection.userId != null) {
                message = {
                    type: "userConnectionIdentificationEvent",
                    connectionId: connectionId,
                    userId: userConnection.userId
                };
                commands.push(message);
            }

            for (let key of Object.keys(userConnection.fields)) {
                let value = userConnection.fields[key];
                message = {
                    type: "userConnectionAddField",
                    connectionId: connectionId,
                    key: key,
                    value: value
                };
                commands.push(message);
            }

            for (let channel of userConnection.streams) {
                message = {
                    type: "userConnectionSubscribe",
                    connectionId: connectionId,
                    userId: userConnection.userId,
                    channel: channel
                };
                commands.push(message);
            }
        }
        return commands;
    }

    userConnectionNewEvent(connectionId) {
        let userConnection = {
            userId: null,
            streams: [],
            fields: {}
        };
        this.userConnection.set(connectionId, userConnection);

        let message = {
            type: "userConnectionNewEvent",
            connectionId: connectionId
        };

        this.sendMessage(message);
    }

    userConnectionAddField(connectionId, key, value) {
        if (value == undefined) {
            value = null;
        }

        let userConnection = this.userConnection.get(connectionId);
        userConnection.fields[key] = value;
        this.userConnection.set(connectionId, userConnection);

        let message = {
            type: "userConnectionAddField",
            connectionId: connectionId,
            key: key,
            value: value
        };

        this.sendMessage(message);
    }

    userConnectionIdentificationEvent(connectionId, userId) {
        let userConnection = this.userConnection.get(connectionId);
        if (userConnection == undefined) {
            Glue.logger.critical("Establishment::MetadataBridge: userConnectionIdentificationEvent before " +
                                 "userConnectionNewEvent! This should NEVER happen!");
            return;
        }
        userConnection.userId = userId;
        this.userConnection.set(connectionId, userConnection);

        let message = {
            type: "userConnectionIdentificationEvent",
            connectionId: connectionId,
            userId: userId
        };

        this.sendMessage(message);
    }

    userConnectionSubscribe(connectionId, userId, channel) {
        let userConnection = this.userConnection.get(connectionId);
        if (userConnection == undefined) {
            Glue.logger.critical("Establishment::MetadataBridge: userConnectionSubscribe before " +
                                 "useConnectionNewEvent! This should NEVER happen!");
            return;
        }
        userConnection.streams.push(channel);
        this.userConnection.set(connectionId, userConnection);

        let message = {
            type: "userConnectionSubscribe",
            connectionId: connectionId,
            userId: userId,
            channel: channel
        };

        this.sendMessage(message);
    }

    userConnectionDestroyEvent(connectionId, userId) {
        this.userConnection.delete(connectionId);

        let message = {
            type: "userConnectionDestroyEvent",
            connectionId: connectionId,
            userId: userId
        };

        this.sendMessage(message);
    }
}

module.exports = MetadataBridge;