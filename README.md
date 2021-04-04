# Bug reproduction: elasticmq send/receive in batches

I really hope that it's just me doing something in a wrong way. 

## Scenario

The code inside creates 2 queues - the main one and the DLQ. The target is either an 
embedded elasticmq instance or AWS SQS. Then, it generates and sends messages of 2 types 
to the main queue in batches. There is a background thread that polls the queue, splits 
the events by type and either deletes events in batches or changes their visibility timeout (sets
1 hour). 

Then, after all generated events are sent, the code starts to wait until all sent events 
are received by the polling thread and processed - either deleted or changed.

## Problem

With enough iterations of the scenario above, the execution fails because some expected 
messages are missing in the queue. It can also be seen with enabled debug logging. The number 
of `org.elasticmq.actor.queue.QueueActor [] - my-queue: Sent message with id ...` and 
`org.elasticmq.actor.queue.QueueActor [] - my-queue: Receiving message` log messages in 
the same test iteration don't match. 

I tried running the same code with SQS, and didn't notice message loss. 

## Execution

To start the test with the embedded elasticmq server run the following command:
```shell
./gradlew run
```
It runs with default params:
- `--mode elasticmq`
- `--iterations 100`
- `--de 100` number of generated events to delete
- `--re 150` number of generated events to change the visibility timeout
- `--await-duration '15 seconds'` amount of time to wait for all expected events in each test iteration

To alter the parameters, use the `--args="..."` gradle parameter:
```shell
./gradlew run --args="--iterations 50 --de 200"
```

It is possible to run the same code with SQS. To make it happen, you need to authenticate
in AWS firstly. Then, when you have valid credentials data in your `./aws/credentials`, you
may run the test by specifying the aws profile name:
```shell
./gradlew run --args="--mode sqs --aws-profile my-profile --aws-region EU_WEST_1"
```

To run the test with the debug level enabled add `--debug` to the arguments.
