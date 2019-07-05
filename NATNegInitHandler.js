function NATNegInitHandler(server_event_listener, redis_connection) {
    this.server_event_listener = server_event_listener;
    this.redis_connection = redis_connection;
    this.DEADBEAT_TIMEOUT = 60000;
    this.REDIS_EXPIRE = 60;
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

NATNegInitHandler.prototype.checkInitComplete = function(message, opposite_index) {
    return new Promise(function(resolve, reject) {
        var required_addresses = this.getNumRequiredAddresses(message);
        var index = message.data.clientindex;
        if(opposite_index) {
            index = message.data.clientindex == 0 ? 1 : 0;
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
                addresses.push(JSON.parse(result[i]));
            }
            resolve(addresses);
        }.bind(this));
    }.bind(this));
}

NATNegInitHandler.prototype.sendConnectionSummaryToClients = function(first_client, second_client, connectCallback) {
    var msg = {};
    msg.type = "connect";

    msg.to_address = first_client[1].from_address;
    msg.driver_address = first_client[1].driver_address;
    msg.data = second_client;
    //console.log(JSON.stringify(msg));
    this.server_event_listener.sendChannelMessage(Buffer.from(JSON.stringify(msg)));

    var resendCallback = function(message) {
        this.server_event_listener.sendChannelMessage(Buffer.from(JSON.stringify(message)));
    }.bind(this, msg);
    connectCallback(msg, resendCallback);

    
    msg.to_address = second_client[1].from_address;
    msg.driver_address = second_client[1].driver_address;
    msg.data = first_client;
    //console.log(JSON.stringify(msg));
    this.server_event_listener.sendChannelMessage(Buffer.from(JSON.stringify(msg)));

    resendCallback = function(message) {
        this.server_event_listener.sendChannelMessage(Buffer.from(JSON.stringify(message)));
    }.bind(this, msg);
    connectCallback(msg, resendCallback);
}

NATNegInitHandler.prototype.handleInitMessage = async function (message, connectCallback) {
    var redis_init_key = message.cookie + "_" + message.data.clientindex + "_init_packets";
    var hset_key = message.data.porttype;
    var storage_data = JSON.stringify(message);
    this.redis_connection.hset(redis_init_key, hset_key, storage_data);
    this.redis_connection.expire(redis_init_key, this.REDIS_EXPIRE);

    var complete = await this.checkInitComplete(message) && await this.checkInitComplete(message, true);

    var key = message.cookie + message.data.clientindex;

    if(complete) {
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
            this.pending_connections[key] = setTimeout(function(deadbeat_info, interval_key) {
                var msg = {};
    
                msg.type = "connect";        
                msg.version = deadbeat_info.version;
                msg.to_address = deadbeat_info.from_address;
                msg.driver_address = deadbeat_info.driver_address;
                msg.data = {finished: 1, cookie: deadbeat_info.cookie};

                this.server_event_listener.sendChannelMessage(Buffer.from(JSON.stringify(msg)));
                delete this.pending_connections[interval_key];
            }.bind(this, message, key), this.DEADBEAT_TIMEOUT);
        }
    }
}
module.exports = NATNegInitHandler;