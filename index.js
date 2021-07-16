var ServerEventListener = require('./NATNegEventListener');
var NATNegInitHandler = require('./NATNegInitHandler');
var redis = require('redis');
const redisURL = process.env.REDIS_URL || "redis://127.0.0.1";
var connection = redis.createClient(redisURL);
var server_event_listener = new ServerEventListener(process.env.RABBITMQ_URL || "amqp://guest:guest@localhost", serverEventHandler);
var init_handler = new NATNegInitHandler(server_event_listener, connection);

var resend_data = {};

const MAX_RESEND = 5;

//TODO: connect resend
function onConnection(msg, resendCallback) {
    var key = msg.to_address + "-" + msg.driver_address;
    if(resend_data[key] !== undefined) {
        clearInterval(resend_data[key].interval);
        delete resend_data[key];
    }
    var entry = {message: msg, resendCallback, resendCount: 0};
    entry.interval = setInterval(function(entryKey, callback) {
        resend_data[entryKey].resendCount++;
        if(resend_data[entryKey].resendCount >= MAX_RESEND) {
            clearInterval(resend_data[entryKey].interval);
            delete resend_data[entryKey];
            return;
        }
        callback();
    }.bind(null, key, resendCallback), 1500);
    resend_data[key] = entry;
}

function serverEventHandler(message) {
    switch(message.type) {
        case 'init':
            //console.log("GOT INIT", message.data, message.from_address);
            init_handler.handleInitMessage(message, onConnection);
        break;
        case 'connect_ack':
            var key = message.from_address + "-" + message.driver_address;            
            init_handler.CleanupConnection(message.cookie, message.clientindex);
            if(resend_data[key]) {
                clearInterval(resend_data[key].interval);
                delete resend_data[key];
            }
            
        break;
    }
    //console.log(message);
}