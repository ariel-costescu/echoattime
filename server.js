import http from 'http';

import { getPostBodyAsJson } from './http-utils.js';
import { RedisController } from './redis-controller.js';
import { ConsoleMessageProcessor } from './message-processor.js';

const PORT = process.env.PORT;
const POLLING_INTERVAL_SECONDS = process.env.POLLING_INTERVAL_SECONDS;

const REDIS_HOST = process.env.REDIS_HOST;
const REDIS_PORT = process.env.REDIS_PORT;

const messageProcessor = new ConsoleMessageProcessor();
const redisController = new RedisController(REDIS_HOST, REDIS_PORT, messageProcessor);

const echoAtTimeHandler = (params, res) => {
    const time = new Date(params.time);
    const message = params.message;

    redisController.streamNewMessage(time, message);

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

    redisController.initStream();
    redisController.recoverUnprocessedMessages();

    setInterval(() => {
        redisController.pollForNewMessages();
    }, POLLING_INTERVAL_SECONDS * 1000);
});