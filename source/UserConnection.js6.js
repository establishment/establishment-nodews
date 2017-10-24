const cookieParser = require("cookie");

const {RedisConnectionPool, Queue, Util, MathEx} = require("establishment-node-core");
const {Glue, RedisStreamPublisher} = require("establishment-node-service-core");

let uidToUserConnection = new Map();

class UserConnection {
    constructor(config, webSocket, redisDispatcher, uidFactory, permissionChecker, metadataObserver, metadataBridge,
                redisCache, permissionDispatcher) {
        this.heartbeatMessage = config.heartbeat.message;
        this.heartbeatIntervalMin = config.heartbeat.interval.min;
        this.heartbeatIntervalMax = config.heartbeat.interval.max;
        this.redisConnection = RedisConnectionPool.getSharedConnection(config.redis.address);

        this.webSocket = webSocket;
        this.redisDispatcher = redisDispatcher;
        this.permissionChecker = permissionChecker;
        this.metadataObserver = metadataObserver;
        this.metadataBridge = metadataBridge;
        this.redisCache = redisCache;
        this.permissionDispatcher = permissionDispatcher;
        this.needUserIdTaskQueue = new Queue();

        this.webSocket.on("message", (message) => {
            this.webSocketProcessMessage(message);
        });

        this.webSocket.on("error", (err) => {
            Glue.logger.error("Establishment::UserConnection: closing connection because of websocket error: " + err);
            this.destroy();
        });

        this.webSocket.on("close", () => {
            this.destroy();
        });

        this.cookie = this.webSocket.upgradeReq.headers.cookie;
        this.ip = this.webSocket.upgradeReq.headers["real_ip"];
        this.connectionTime = Util.getUnixTime();

        this.crossSessionId = null;

        this.userId = -1;
        this.uid = -1;
        uidFactory.requestUID( (uid) => {
            this.processAssignedUID(uid);
        });

        this.heartbeatInterval = null;

        this.streamLastIndex = new Map();

        this.resetHeartbeat();
    }

    getSession() {
        return this.crossSessionId;
    }

    getUserId() {
        return this.userId;
    }

    webSocketProcessMessage(message) {
        if (this.webSocket == null) {
            return;
        }

        let token = message.split(" ");

        if (token[0] == "s" && token.length == 2) {
            // Glue.logger.info("Establishment::UserConnection: " + this.ip + ": want to subscribe to channel '" + token[1] + "'");
            this.requestPermissionSafe(token[1]);
        } else if (token[0] == "u" && token.length == 2) {
            // Glue.logger.info("Establishment::UserConnection: " + this.ip + ": want to unsubscribe from channel '" + token[1] + "'");
            this.redisDispatcher.unsubscribe(token[1], this);
        } else if (token[0] == "r" && token.length == 3) {
            // Glue.logger.info("Establishment::UserConnection: " + this.ip + ": want to resubscribe to channel '" + token[2] + "' from index " + token[1]);
            let index = parseInt(token[1]);
            this.streamLastIndex.set(token[2], index);
            this.requestPermissionSafe(token[2]);
        } else {
            Glue.logger.error("Establishment::UserConnection: " + this.ip + ": invalid command!");
        }
    }

    handleWebSocketError(error) {
        // Consume already triggered errors
        if (this.webSocket == null) {
            return;
        }
        if (error != null) {
            Glue.logger.error("Establishment::UserConnection: Websocket send error: " + error);
            if (error == "Error: This socket has been ended by the other party" || error == "Error: not opened") {
                Glue.logger.error("Establishment::UserConnection: Websocket force-close.");
                this.destroy();
            }
        }
    }

    send(message) {
        if (this.webSocket != null) {
            this.resetHeartbeat();
            this.webSocket.send(message, (error) => {
                this.handleWebSocketError(error);
            });
        }
    }

    sendCommand(command) {
        this.send("c " + command);
    }

    sendFatalError(message) {
        this.send("efc " + message);
    }

    resetHeartbeat() {
        if (this.heartbeatInterval != null) {
            clearTimeout(this.heartbeatInterval);
        }
        this.heartbeatInterval = setTimeout(() => {
            this.send(this.heartbeatMessage);
        }, MathEx.random(this.heartbeatIntervalMin, this.heartbeatIntervalMax));
    }

    forwardRedisMessage(channel, message) {
        this.send("m " + channel + " " + message);
    }

    requestPermissionSafe(channel) {
        if (this.userId == -1) {
            let task = {"func": "request-permission", "param0": channel};
            this.needUserIdTaskQueue.push(task);
        } else {
            this.requestPermission(channel);
        }
    }

    requestPermission(channel) {
        this.permissionDispatcher.registerPermission(this, channel);
        this.permissionChecker.requestPermission(this.userId, channel);
    }

    resendSingleStreamMessage(channel, index) {
        this.redisCache.get(RedisStreamPublisher.getStreamMessageIdPrefix(channel) + index, (error, reply) => {
            if (error != null) {
                Glue.logger.error("Establishment::UserConnection: failed to get stream message with id " + index +
                                  " for #" + channel + " error: " + error);
                return;
            }
            if (this.webSocket == null) {
                return;
            }
            if (reply == null) {
                this.send("m " + channel + " n " + index);
                return;
            }
            this.forwardRedisMessage(channel, "i " + index + " " + reply);
        });
    }

    resendStreamMessages(channel, index) {
        ++index;
        this.redisConnection.get(RedisStreamPublisher.getStreamIdCounter(channel), (error, reply) => {
            if (error != null) {
                Glue.logger.error("Establishment::UserConnection: failed to get stream id counter for #" + channel +
                                  " error: " + error);
                return;
            }
            if (this.webSocket == null) {
                return;
            }
            for (;index <= reply; ++index) {
                this.resendSingleStreamMessage(channel, index);
            }
        });
    }

    processPermissionResult(channel, result, reason) {
        if (this.webSocket == null) {
            return;
        }
        if (result) {
            this.send("s " + channel);
            // send might return with error and close the connection ("destroying" the UserConnection object)
            if (this.webSocket == null) {
                return;
            }
            this.redisDispatcher.subscribe(channel, this);
            this.metadataBridge.userConnectionSubscribe(this.uid, this.userId, channel);
            //Glue.logger.info("Establishment::UserConnection: permission to subscribe #" + channel + " accepted! (" + reason + ")");

            if (this.streamLastIndex.has(channel)) {
                let index = this.streamLastIndex.get(channel);
                this.streamLastIndex.delete(channel);
                this.resendStreamMessages(channel, index);
            }
        } else {
            this.send("error invalidSubscription " + channel + " " + this.userId);
            //Glue.logger.warn("Establishment::UserConnection: permission to subscribe #" + channel + " declined! (" + reason + ")");
        }
    }

    requestIdentification() {
        if (this.crossSessionId == null) {
            Glue.logger.error("Establishment::UserConnection: requestIdentification called but there is no crossSessionId!");
            return;
        }
        this.permissionDispatcher.registerIdentification(this);
        this.permissionChecker.requestIdentification(this.crossSessionId);
    }

    processIdentificationResult(userId) {
        if (userId == -1) {
            // TODO: might want to change the key that enables guest connections
            if (Glue.registryKeeper.get("enable-guests") != "false") {
                userId = 0;
            } else {
                let error = {
                    message: "Decline websocket connection!",
                    reason: "Invalid crossSessionId"
                };
                this.sendFatalError(JSON.stringify(error));
                this.destroy();
                return;
            }
        }

        this.userId = userId;

        if (this.userId == 0) {
            let guestConnectionLimit = Glue.registryKeeper.get("max-guests");
            guestConnectionLimit = parseInt(guestConnectionLimit);
            if (guestConnectionLimit > 0) {
                let totalGuestConnections = this.metadataObserver.getTotalGuestConnections();
                if (totalGuestConnections >= guestConnectionLimit) {
                    Glue.logger.warning("Establishment::UserConnection: max number of guest conenction reached (" +
                                        totalGuestConnections + "/" + guestConnectionLimit + ")");
                    this.userId = -1;
                    this.destroy();
                    return;
                }
            }
        }

        this.metadataBridge.userConnectionIdentificationEvent(this.uid, this.userId);

        this.executeDelayedWork(this.needUserIdTaskQueue);
    }

    processAssignedUID(uid) {
        if (this.webSocket == null) {
            return;
        }

        this.uid = uid;
        uidToUserConnection.set(this.uid, this);
        this.metadataBridge.userConnectionNewEvent(this.uid);
        this.metadataBridge.userConnectionAddField(this.uid, "IP", this.ip);
        this.metadataBridge.userConnectionAddField(this.uid, "userAgent",
                                                   this.webSocket.upgradeReq.headers["user-agent"]);
        this.metadataBridge.userConnectionAddField(this.uid, "cookie", this.cookie);
        this.metadataBridge.userConnectionAddField(this.uid, "acceptLanguage",
                                                   this.webSocket.upgradeReq.headers["accept-language"]);
        this.metadataBridge.userConnectionAddField(this.uid, "connectionTime", this.connectionTime);

        //Glue.logger.info("Establishment::UserConnection: " + this.ip + ": Assigned UID = " + this.uid);

        if (this.cookie == null) {
            this.processIdentificationResult(0); // always treat connections without cookie as guest-connections
            return;
        }

        let cookies = cookieParser.parse(this.cookie);

        if (!cookies.hasOwnProperty("crossSessionId")) {
            this.processIdentificationResult(0); // always treat connections without crossSessionId as guest-connections
            return;
        }

        if (Glue.registryKeeper.get("enable-csrf") == "true") {
            if (!cookies.hasOwnProperty("csrftoken")) {
                // TODO: should check csrftoken against Django's map
                this.processIdentificationResult(-1);
                return;
            }
        }

        this.crossSessionId = cookies.crossSessionId;

        this.requestIdentification();
    }

    executeDelayedWork(taskQueue) {
        while (!taskQueue.empty()) {
            let request = taskQueue.pop();
            if (request["func"] == "request-permission") {
                this.requestPermission(request["param0"]);
            } else {
                Glue.logger.critical("Establishment::UserConnection: undefined request-func: " + request["func"]);
            }
        }
    }

    destroy() {
        if (this.webSocket == null) {
            return;
        }

        this.permissionDispatcher.unregister(this);

        if (this.uid != -1) {
            this.metadataBridge.userConnectionDestroyEvent(this.uid, this.userId);
        }

        this.webSocket.close();

        this.redisDispatcher.unsubscribeFromAll(this);
        if (this.uid != -1) {
            uidToUserConnection.delete(this.uid);
        }

        if (this.heartbeatInterval != null) {
            clearTimeout(this.heartbeatInterval);
            this.heartbeatInterval = null;
        }

        this.webSocket = null;

        this.heartbeatMessage = null;
        this.heartbeatIntervalMin = null;
        this.heartbeatIntervalMax = null;
        this.redisConnection = null;

        this.redisDispatcher = null;
        this.permissionChecker = null;
        this.metadataObserver = null;
        this.metadataBridge = null;
        this.redisCache = null;

        this.needUserIdTaskQueue = null;
        this.cookie = null;
        this.crossSessionId = null;
        this.ip = null;

        this.connectionTime = null;
        this.uid = null;

        this.userId = null;
        this.streamLastIndex = null;
    }

    static findByUID(uid) {
        if (uidToUserConnection.has(uid)) {
            return uidToUserConnection.get(uid);
        }
        return null;
    }

    static getAll() {
        return uidToUserConnection.values();
    }

    static getGuestId() {
        if (Glue.registryKeeper.get("enable-guests") == "true") {
            return 0;
        }
        return -1;
    }
}

module.exports = UserConnection;
