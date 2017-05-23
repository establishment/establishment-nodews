const {RedisCache} = require("establishment-node-core");
const {MetadataObserver, UniqueIdentifierFactory, PermissionChecker,
       PermissionDispatcher} = require("establishment-node-service-core");

const RedisDispatcher = require("./RedisDispatcher.js6.js");
const UserConnection = require("./UserConnection.js6.js");
const MetadataBridge = require("./MetadataBridge.js6.js");

const WebSocketServer = require("ws").Server;

class WebsocketServer {
    constructor(config) {
        this.config = config;
        this.redisDispatcher = new RedisDispatcher(config.redisDispatcher);
        this.uidFactory = new UniqueIdentifierFactory(config.uidFactory);
        this.permissionChecker = new PermissionChecker(config.permissionChecker);
        this.permissionDispatcher = new PermissionDispatcher();
        this.metadataObserver = new MetadataObserver(config.metadataObserver);
        this.metadataBridge = new MetadataBridge(config.metadataBridge, this.uidFactory);
        this.redisCache = new RedisCache(config.redisCache);

        this.webSocketServer = null;

        this.permissionChecker.link(this.permissionDispatcher);
    }

    start() {
        this.webSocketServer = new WebSocketServer(this.config.listen);

        this.run();
    }

    run() {
        this.webSocketServer.on("connection", (webSocket, req) => {
            webSocket.upgradeReq = req;
            new UserConnection(this.config.userConnection, webSocket, this.redisDispatcher, this.uidFactory,
                               this.permissionChecker, this.metadataObserver, this.metadataBridge, this.redisCache,
                               this.permissionDispatcher);
        });
    }
}

module.exports = WebsocketServer;
