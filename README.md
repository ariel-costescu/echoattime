# echoattime
Microservice that prints a message to the console at a given date and time in the future.

## Implementation

To handle a large volume of messages, it uses Redis streams with consumer groups to fan out incoming messages across multiple cluster nodes.

For simplicity, it assumes there is a fixed number of cluster nodes with each node.js server instance corresponding to a node with a unique SHARD_ID, and a consumer group of the same id. In production, an external system like a load balancer would manage cluster nodes and SHARD_IDs, but here we're just reading the id from an .env file.

Consumer groups ensure that each incoming message is assigned to a single server instance, so that a message can only be echoed once.

Redis streams also have the benefit of adding resilience to node failure/restart, by keeping track of pending messages and allowing them to be reprocessed when message delivery fails.

On server startup, each node attempts to recover from a previous failure/restart by processing any pending messages that were not acknowledged.

It then continuously polls for incoming messages assigned to its consumer group. Any messages that are in the past are processed immediately and printed to the console, otherwise they are added to a sorted set that is unique to the current node, using time as score and including the stream entry id for later processing.

The sorted set allows for efficient lookup of messages in the past on subsequent polls. As time progresses, messages that become ready for processing are printed to the console and removed from the sorted set, while also acknowledging to the stream to prevent processing the same message more than once.

## Running on local
Just do a `docker compose up` to get a local Redis instance running and start the server with `npm run start`.
