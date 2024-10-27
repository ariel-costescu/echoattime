import http from 'http';
import { Redis } from 'ioredis';

import { getPostBodyAsJson } from './http-utils.js';

const PORT = process.env.PORT;
const POLLING_INTERVAL_SECONDS = process.env.POLLING_INTERVAL_SECONDS;
const SHARD_ID = process.env.SHARD_ID;

const redis = new Redis();

const STREAM_KEY = "messageStream";
const CONSUMER_GROUP_ID = "messageConsumers";
const PENDING_MESSAGES_KEY = `${SHARD_ID}-queue`;

redis.xinfo("STREAM", "messageStream").then(null, (err) => {
    redis.xgroup("CREATE", STREAM_KEY, CONSUMER_GROUP_ID, "$", "MKSTREAM");
    redis.xgroup("CREATECONSUMER", STREAM_KEY, CONSUMER_GROUP_ID, SHARD_ID);
});

const echoAtTimeHandler = (params, res) => {
    let time = new Date(params.time);
    let message = params.message;

    redis.xadd(STREAM_KEY, "*", "time", time, "message", message);

    res.statusCode = 201;
    res.end();
}

const server = http.createServer((req, res) => {
    if (req.url === '/echoAtTime') {
        getPostBodyAsJson(req, (params) => {
            echoAtTimeHandler(params, res);
        })
    } else {
        res.statusCode = 404;
        res.end('Unknown resource');
    }
});

server.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});

const processStreamResults = (results) => {
    if (results) {
        const [key, messages] = results[0];
        if (messages) {
            messages.forEach(msg => {
                if (msg) {
                    const [msgId, msgFields] = msg;
                    const msgTime = new Date(msgFields[1]);
                    const echoedMsg = msgFields[3];
                    const now = new Date();
                    if (msgTime.getTime() <= now.getTime()) {
                        console.log(`'${echoedMsg}'`);
                        redis.xack(STREAM_KEY, CONSUMER_GROUP_ID, msgId);
                    } else {
                        const futureMsg = {
                            "msgId": msgId,
                            "msgTime": msgTime,
                            "echoedMsg": echoedMsg
                        }
                        const msgScore = msgTime.getTime();
                        redis.zadd(PENDING_MESSAGES_KEY, msgScore, JSON.stringify(futureMsg));
                    }
                }
            });
        }
    }
}

const processPastMessages = (messages) => {
    if (messages?.length > 0) messages.forEach(msg => {
        const pastMsg = JSON.parse(msg);
        console.log(`'${pastMsg.echoedMsg}'`);
        redis.xack(STREAM_KEY, CONSUMER_GROUP_ID,pastMsg.msgId);
        redis.zrem(PENDING_MESSAGES_KEY, msg);
    });
}

// On server startup, recover any unprocessed stream messages
redis.xreadgroup("GROUP", CONSUMER_GROUP_ID, SHARD_ID, "STREAMS", STREAM_KEY, "0")
        .then(processStreamResults);  

// Poll for new messages from the stream, processing them immediately if they are in the past
// or add them to a sorted set for later processing
setInterval(() => {
    redis.xreadgroup("GROUP", CONSUMER_GROUP_ID, SHARD_ID, "STREAMS", STREAM_KEY, ">")
        .then(processStreamResults);
    redis.zrangebyscore(PENDING_MESSAGES_KEY, 0, Date.now())
        .then(processPastMessages);
}, POLLING_INTERVAL_SECONDS * 1000);