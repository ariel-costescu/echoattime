import http from 'http';
import { Redis } from 'ioredis';

import { getPostBodyAsJson } from './http-utils.js';

const PORT = process.env.PORT;
const POLLING_INTERVAL_SECONDS = process.env.POLLING_INTERVAL_SECONDS;

const redis = new Redis();

const echoAtTimeHandler = (params, res) => {
    let time = new Date(params.time);
    console.log(`time is '${time.toUTCString()}'`);
    let message = params.message;
    console.log(`message is "${message}"`);

    redis.zadd("messageQueue", time.getTime(), JSON.stringify(params));

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

setInterval(async () => {
    const results = await redis.zrangebyscore("messageQueue", 0, new Date().getTime());
    if (results) results.forEach((result) => {
        const msg = JSON.parse(result);
        console.log(msg);
        redis.zrem("messageQueue", result);
    });
    
}, POLLING_INTERVAL_SECONDS * 1000);