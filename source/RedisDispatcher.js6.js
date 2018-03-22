const {RedisConnectionPool} = require("establishment-node-core");
const {Glue} = require("establishment-node-service-core");

class RedisDispatcher {
    constructor(config) {
        this.config = config;
        this.redisClient = RedisConnectionPool.getConnection(config.redis);
        this.streamToUserConnection = new Map();
        this.userConnectionToStream = new Map();
        this.rawMessageHandler = (...args) => {
            this.defaultRawMessageHandler(...args);
        };

        if (this.config.hasOwnProperty("options")) {
            if (this.config.options.hasOwnProperty("rawMessageHandler")) {
                if (this.config.options.rawMessageHandler == "treatAsVanilla") {
                    this.rawMessageHandler = (...args) => {
                        this.rawMessageHandlerTreatAsVanilla(...args);
                    };
                } else if (this.config.options.rawMessageHandler == "error") {
                    this.rawMessageHandler = (...args) => {
                        this.rawMessageHandlerError(...args);
                    };
                } else if (this.config.options.rawMessageHandler == "suppressError") {
                    this.rawMessageHandler = (...args) => {
                        this.rawMessageHandlerSuppressError(...args);
                    };
                } else if (this.config.options.rawMessageHandler == "passRaw") {
                    this.rawMessageHandler = (...args) => {
                        this.rawMessageHandlerPassRaw(...args);
                    };
                }
            }
        }

        this.redisClient.on("error", (err) => {
            Glue.logger.error("Establishment::RedisDispatcher: " + err);
        });

        this.redisClient.on("ready", () => {
            Glue.logger.info("Establishment::RedisDispatcher ready!");
        });

        this.redisClient.on("subscribe", (channel, count) => {
            // TODO: keep track globally of the number of submissions
            // Glue.logger.info("Establishment::RedisDispatcher: subscribe to #" + channel + ", " + count +
            //                  " total subscriptions");
        });

        this.redisClient.on("unsubscribe", (channel, count) => {
            // Glue.logger.info("Establishment::RedisDispatcher: no more UserConnection listening to #" + channel +
            //                  ", unsubscribe. Total subscriptions remaining: " + count);
        });

        this.redisClient.on("message", (channel, message) => {
            this.processStreamMessage(channel, message);
        });
    }

    processStreamMessage(channel, message) {
        let firstSpace = message.indexOf(' ');
        let type = message.substr(0, firstSpace);
        let withoutType = message.substr(firstSpace + 1);
        if (type == "i") {
            let secondSpace = withoutType.indexOf(' ');
            let index = parseInt(withoutType.substr(0, secondSpace));
            let content = withoutType.substr(secondSpace + 1);
            this.dispatchIndexed(channel, index, content);
        } else if (type == "v") {
            this.dispatchVanilla(channel, withoutType);
        } else {
            this.rawMessageHandler(channel, message);
        }
    }

    dispatch(channel, message) {
        if (this.streamToUserConnection.has(channel)) {
            let userConnections = Array.from(this.streamToUserConnection.get(channel));
            for (let userConnection of userConnections) {
                userConnection.forwardRedisMessage(channel, message);
            }
        }
    }

    dispatchIndexed(channel, index, content) {
        this.dispatch(channel, "i " + index + " " + content);
    }

    dispatchVanilla(channel, content) {
        this.dispatch(channel, "v " + content);
    }

    rawMessageHandlerError(channel, message) {
        Glue.logger.critical("Establishment::RedisDispatcher: invalid stream message type <RAW_MESSAGE>");
    }

    rawMessageHandlerTreatAsVanilla(channel, message) {
        this.dispatchVanilla(channel, message);
    }

    rawMessageHandlerPassRaw(channel, message) {
        this.dispatch(channel, message);
    }

    rawMessageHandlerSuppressError(channel, message) {
    }

    subscribe(channel, userConnection) {
        let userConnections;

        if (this.streamToUserConnection.has(channel)) {
            userConnections = this.streamToUserConnection.get(channel);
        } else {
            userConnections = new Set();
            this.redisClient.subscribe(channel);
            this.streamToUserConnection.set(channel, userConnections);
        }
        userConnections.add(userConnection);

        let streams;

        if (this.userConnectionToStream.has(userConnection)) {
            streams = this.userConnectionToStream.get(userConnection);
        } else {
            streams = new Set();
            this.userConnectionToStream.set(userConnection, streams);
        }
        streams.add(channel);
    }

    unsubscribe(channel, userConnection) {
        if (this.streamToUserConnection.has(channel)) {
            let userConnections = this.streamToUserConnection.get(channel);
            userConnections.delete(userConnection);
            if (userConnections.size == 0) {
                //TODO: add remanence to optimize single-tab users
                //Glue.logger.info("Establishment::RedisDispatcher: no more listeners on #" + channel + " (unsubscribe)");
                this.streamToUserConnection.delete(channel);
                this.redisClient.unsubscribe(channel);
            }
        } else {
            Glue.logger.error("Establishment::RedisDispatcher: stream-to-userconnection does not contain channel #" + channel);
        }

        if (this.userConnectionToStream.has(userConnection)) {
            let streams = this.userConnectionToStream.get(userConnection);
            streams.delete(channel);
            if (streams.size == 0) {
                // Glue.logger.info("Establishment::RedisDispatcher: UserConnection no longer subscribed to any channel!");
                this.userConnectionToStream.delete(channel);
            }
        } else {
            Glue.logger.error("Establishment::RedisDispatcher: userconnection-to-stream does not contain uid=" + userConnection.uid);
        }
    }

    unsubscribeFromAll(userConnection) {
        if (!this.userConnectionToStream.has(userConnection)) {
            return;
        }
        let channels = Array.from(this.userConnectionToStream.get(userConnection));

        for (let channel of channels) {
            this.unsubscribe(channel, userConnection);
        }
    }
}

RedisDispatcher.prototype.defaultRawMessageHandler = RedisDispatcher.prototype.rawMessageHandlerError;

module.exports = RedisDispatcher;
