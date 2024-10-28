import { Redis } from 'ioredis';

const STREAM_KEY = "messageStream";
const CONSUMER_GROUP_ID = "messageConsumers";
const SHARD_ID = process.env.SHARD_ID;
const PENDING_MESSAGES_KEY = `${SHARD_ID}-queue`;

function RedisController(host, port, messageProcessor) {
    const redis = new Redis(port, host);   

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
                            messageProcessor.process(msgTime, echoedMsg);
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
    };
    
    const processPastMessages = (messages) => {
        if (messages?.length > 0) messages.forEach(msg => {
            const pastMsg = JSON.parse(msg);
            messageProcessor.process(pastMsg.msgTime, pastMsg.echoedMsg);
            redis.xack(STREAM_KEY, CONSUMER_GROUP_ID,pastMsg.msgId);
            redis.zrem(PENDING_MESSAGES_KEY, msg);
        });
    };

    return {
        initStream: async () => {
            const existingGroup = await redis.xinfo("GROUPS", STREAM_KEY)
                .catch(err => {});
            if (!existingGroup) {
                await redis.xgroup("CREATE", STREAM_KEY, CONSUMER_GROUP_ID, "$", "MKSTREAM");
            }
            const existingConsumers = await redis.xinfo("CONSUMERS", STREAM_KEY, CONSUMER_GROUP_ID)
                .catch(err => {});
            if (!existingConsumers || !existingConsumers.includes(SHARD_ID)) {
                await redis.xgroup("CREATECONSUMER", STREAM_KEY, CONSUMER_GROUP_ID, SHARD_ID);
            } 
        },

        recoverUnprocessedMessages: () => {
            redis.xreadgroup("GROUP", CONSUMER_GROUP_ID, SHARD_ID, "STREAMS", STREAM_KEY, "0")
                .then(processStreamResults); 
        },
        
        pollForNewMessages: () => {
            redis.xreadgroup("GROUP", CONSUMER_GROUP_ID, SHARD_ID, "STREAMS", STREAM_KEY, ">")
                .then(processStreamResults);
            redis.zrangebyscore(PENDING_MESSAGES_KEY, 0, Date.now())
                .then(processPastMessages);
        },

        streamNewMessage: (time, message) => {
            redis.xadd(STREAM_KEY, "*", "time", time, "message", message);
        }
    }
}

export {RedisController}