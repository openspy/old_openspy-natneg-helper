function NATNegInitHandler(server_event_listener, redis_connection) {
    this.server_event_listener = server_event_listener;
    this.redis_connection = redis_connection;

    this.DEADBEAT_TIMEOUT = 60;

    this.pending_connections = {};
}
NATNegInitHandler.prototype.getNumRequiredAddresses = function(message) {
    var required_addresses = 0;
    if(message.data.usegameport > 0) {
        required_addresses++;
    }
    if(message.version >= 2) {
        required_addresses += 2;
    }
    if(message.version >= 3) {
        required_addresses++;
    }
    return required_addresses;
}

NATNegInitHandler.prototype.markInitComplete = function(message, opposite_index) {
    return new Promise(function(resolve, reject) {
        var index = message.data.clientindex;
        if(opposite_index) {
            index = message.data.clientindex == 0 ? 1 : 0;
        }
        var redis_key = message.cookie + "_" + index;
        this.redis_connection.hset(redis_key, "connect_complete", "1", function(err, result) {
            this.redis_connection.expire(redis_key, this.DEADBEAT_TIMEOUT);
            return resolve(true);
        }.bind(this));
    }.bind(this));
}
NATNegInitHandler.prototype.checkInitComplete = function(message, opposite_index) {
    return new Promise(async function(resolve, reject) {
        var required_addresses = this.getNumRequiredAddresses(message);
        var index = message.data.clientindex;
        if(opposite_index) {
            index = message.data.clientindex == 0 ? 1 : 0;

            var init_data = await this.GetInitData(message.cookie, index);

            if(!init_data) return resolve(false);
            required_addresses = this.getNumRequiredAddresses({data: init_data});
        }
        var redis_key = message.cookie + "_" + index;
        var redis_init_key = redis_key  + "_init_packets";

        this.redis_connection.hget(redis_key, "connect_complete", function(err, result) {
            if(result != null) return resolve(false);
            this.redis_connection.hlen(redis_init_key, function(err, num_keys) {
                if(num_keys >= required_addresses) {
                    return resolve(true);
                }
                return resolve (false);
            });
        }.bind(this));
    }.bind(this));
}
NATNegInitHandler.prototype.getAllInitPackets = function(cookie, client_index, required_addresses) {
    return new Promise(function(resolve, reject) {
        var redis_init_key = cookie + "_" + client_index + "_init_packets";
        var keys = [];
        for(var i=0;i<required_addresses;i++) {
            keys.push(i.toString());
        }
        this.redis_connection.hmget(redis_init_key, keys, function(err, result) {
            var addresses = [];
            for(var i=0;i<required_addresses;i++) {
                if(result[i] != null)
                    addresses.push(JSON.parse(result[i]));
            }
            resolve(addresses);
        }.bind(this));
    }.bind(this));
}

NATNegInitHandler.prototype.sendConnectionSummaryToClients = function(first_client, second_client, connectCallback) {
    var msg = {};
    msg.type = "connect";
    msg.data = second_client;
    this.sendMessageToAssociatedPairs(first_client[0].cookie, first_client[0].data.clientindex, msg);

    var resendCallback = function(cookie, index, message) {
        this.sendMessageToAssociatedPairs(cookie, index, message);
    }.bind(this, first_client[0].cookie, first_client[0].data.clientindex, msg);
    connectCallback(msg, resendCallback);

    var second_msg = Object.assign({}, msg);

    
    second_msg.data = first_client;
    this.sendMessageToAssociatedPairs(second_client[0].cookie, second_client[0].data.clientindex, second_msg);

    resendCallback = function(cookie, index, message) {
        this.sendMessageToAssociatedPairs(cookie, index, message);
    }.bind(this, second_client[0].cookie, second_client[0].data.clientindex, second_msg);
    connectCallback(second_msg, resendCallback);
}

NATNegInitHandler.prototype.GetInitData = async function(cookie, client_index) {
    return new Promise(function(resolve, reject) {
        var redis_init_key = cookie + "_" + client_index + "_init_packets";
        this.redis_connection.hget(redis_init_key, "1", function(err, result) {
            var jsonResult = JSON.parse(result);
            if(jsonResult == null) return resolve(null);
            resolve(jsonResult.data);
        }.bind(this));
    }.bind(this));
}
NATNegInitHandler.prototype.handleInitMessage = async function (message, connectCallback) {
    var redis_init_key = message.cookie + "_" + message.data.clientindex + "_init_packets";

    await this.associateCookieToClientDriverPair(message.cookie, message.data.clientindex, message.from_address, message.driver_address, message.hostname);

    var hset_key = message.data.porttype;
    var storage_data = JSON.stringify(message);
    this.redis_connection.hset(redis_init_key, hset_key, storage_data);
    this.redis_connection.expire(redis_init_key, this.DEADBEAT_TIMEOUT);

    var complete = await this.checkInitComplete(message) && await this.checkInitComplete(message, true);

    var key = message.cookie + message.data.clientindex;

    if(complete) {
        await this.markInitComplete(message);
        await this.markInitComplete(message, true);

        //call "calculate nat mapping" against address objects for both clients
        var required_addresses = this.getNumRequiredAddresses(message);
        var client_addresses = await this.getAllInitPackets(message.cookie, message.data.clientindex, required_addresses);

        var opposite_index = message.data.clientindex == 0 ? 1 : 0;
        var opposite_client_addresses = await this.getAllInitPackets(message.cookie, opposite_index, required_addresses);

        //send connection info
        this.sendConnectionSummaryToClients(client_addresses, opposite_client_addresses, connectCallback);

        if(this.pending_connections[key]) {
            clearTimeout(this.pending_connections[key]);
            delete this.pending_connections[key];
        }

        key = message.cookie + opposite_index;
        if(this.pending_connections[key]) {
            clearTimeout(this.pending_connections[key]);
            delete this.pending_connections[key];
        }
    } else {
        if(this.pending_connections[key] === undefined) {
            this.pending_connections[key] = setTimeout(async function(deadbeat_info, interval_key) {
                var msg = {};
    
                msg.type = "connect";        
                msg.version = deadbeat_info.version;
                msg.to_address = deadbeat_info.from_address;
                msg.driver_address = deadbeat_info.driver_address;
                msg.data = {finished: 1, cookie: deadbeat_info.cookie};

                await this.sendMessageToAssociatedPairs(deadbeat_info.cookie, deadbeat_info.data.clientindex, msg);
                await this.CleanupConnection(deadbeat_info.cookie, deadbeat_info.data.clientindex);
                delete this.pending_connections[interval_key];
            }.bind(this, message, key), (this.DEADBEAT_TIMEOUT * 1000));
        }
    }
}
NATNegInitHandler.prototype.associateCookieToClientDriverPair = function(cookie, client_index, client_address, driver_address, hostname) {
    return new Promise(function(resolve, reject) {
        var data_obj = {cookie, client_index, client_address, driver_address, hostname};
        var connection_incr_key = "ASSOCINCR_" + cookie + "-" + client_index;
        this.redis_connection.incr(connection_incr_key,function(con_key, peer_data, err, association_index) {
            if(err) return reject();
            this.redis_connection.expire(con_key, this.DEADBEAT_TIMEOUT * 2);
            var sub_key = "peer_" + association_index;
            var assoc_key = "ASSOC-" + peer_data.cookie + "-" + peer_data.client_index;
            this.redis_connection.hset(assoc_key, sub_key, JSON.stringify(peer_data), function(key, err) {
                if(err) return reject();
                this.redis_connection.expire(key, this.DEADBEAT_TIMEOUT * 2);
                resolve();
            }.bind(this, assoc_key));
        }.bind(this, connection_incr_key, data_obj));
    }.bind(this));
}
NATNegInitHandler.prototype.sendMessageToAssociatedPairs = function(cookie, client_index, data) {
    return new Promise(function(resolve, reject) {
        var assoc_key = "ASSOC-" + cookie + "-" + client_index;
        var handleScanResults = null;
        var performScan = function(cursor) {
            this.redis_connection.hscan(assoc_key, cursor, "MATCH", "*", handleScanResults.bind(this));
        }.bind(this);
        handleScanResults = function(err, res) {
            if(err) return reject(err);
            var c = parseInt(res[0]);
            if(res[1] && res[1].length > 0) {
                var idx = 0;
                for(key of res[1]) {
                    if(idx++ % 2) {
                        var peer_data = JSON.parse(key);
                        this.sendMessageToPeer(peer_data, data);  
                    }
                }
            }
            if(c != 0) {
                performScan(c);
            } else {
                resolve();
            }
        }.bind(this);
        performScan(0);
    }.bind(this));
}
NATNegInitHandler.prototype.sendMessageToPeer = function(peer_data, message_data) {
    let send_message = Object.assign({}, message_data);
    send_message.to_address = peer_data.client_address;
    send_message.driver_address = peer_data.driver_address;
    send_message.hostname = peer_data.hostname;
    this.server_event_listener.sendChannelMessage(Buffer.from(JSON.stringify(send_message)));
}
NATNegInitHandler.prototype.CleanupConnection = async function(cookie, client_index) {
    var assoc_key = "ASSOC-" + cookie + "-" + client_index;
    await this.redis_connection.del(assoc_key);

    var connection_incr_key = "ASSOCINCR_" + cookie + "-" + client_index;
    await this.redis_connection.del(connection_incr_key);
}
module.exports = NATNegInitHandler;