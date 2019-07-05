var amqp = require('amqplib/callback_api');

function NATNegEventListener(amqpConnection, eventCallback) {
    this.eventCallback = eventCallback;

    var channelCallback = this.handleChannelMessage.bind(this);

    amqp.connect(amqpConnection, function(err, conn) {
        this.amqpConnection = conn;
        
        var ex = 'openspy.natneg';
        conn.createChannel(function(err, ch) {
            this.channel = ch;
            ch.assertExchange(ex, 'topic', {durable: true});
            ch.assertQueue('', {exclusive: true}, function(err, q) {
                ch.bindQueue(q.queue, ex, 'natneg.core');

                ch.consume(q.queue, function(msg) {
                    if(msg.content && msg.properties.headers.appName == 'natneg') {
                        channelCallback(msg.content.toString());
                    }
                }, {noAck: true});
            });
        }.bind(this));
    }.bind(this));
}

NATNegEventListener.prototype.handleChannelMessage = function(message) {
    var json = JSON.parse(message);
    this.eventCallback(json);
};

NATNegEventListener.prototype.sendChannelMessage = function(message) {
    this.channel.publish("openspy.natneg", "natneg.core", message, {deliveryMode: 2});
}

module.exports = NATNegEventListener;
