package sandbox.scala

import com.typesafe.scalalogging.Logger
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{GetQueueUrlRequest, ReceiveMessageRequest, SendMessageBatchRequest, SendMessageBatchRequestEntry}

import java.util
import java.util.UUID
import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Random, Success, Try, Using}
import scala.jdk.CollectionConverters._
import sandbox.scala.Static._

import scala.jdk.FutureConverters._

class Test(clientMode: ClientMode, iterations: Int, eventsToDeleteNumber: Int, eventsToReturnNumber: Int, awaitDuration: Duration) {
  private val logger = Logger[Test]

  {
    Using(clientMode.initClient()) { client =>
      val createQueueResponse = Await.result(createQueues(client), 10.seconds)
      try {
        logger.info("Created the queue")

        Using(new QueueProcessing(clientMode, createQueueResponse.queueUrl())) { processing =>
          (1 to iterations).foreach { i =>
            logger.info(s"TEST ITERATION ${i} *******************************************************************************")
            sendMessagesAndWait(client, processing, awaitDuration)
          }
          logger.info("ALL ITERATIONS COMPLETED SUCCESSFULLY")
        }.get
        logger.info("Stopped queue processing")
      } finally {
        deleteQueues(client).get()
      }
    }.get
  }

  private def sendMessagesAndWait(client: SqsAsyncClient, processing: QueueProcessing, awaitDuration: Duration): Unit = {
    require(processing.returnedEvents.isEmpty, "Queue container unexpected RETURN events")
    require(processing.deletedEvents.isEmpty, "Queue container unexpected DELETE events")

    val eventsToDelete = (1 to eventsToDeleteNumber).map(_ => new Event(EventType.Delete))
    val eventsToReturn = (1 to eventsToReturnNumber).map(_ => new Event(EventType.Return))

    val expectedDeletedEvents = new util.ArrayList(eventsToDelete.asJava)
    val deletedCompletionAsync = Future {
      while (!expectedDeletedEvents.isEmpty) {
        val event = processing.deletedEvents.takeFirst()
        if (!expectedDeletedEvents.remove(event)) {
          throw new Exception(s"Unexpected event in the Deleted queue: $event")
        }

        logger.info(s"Got awaited Delete event ${event}, ${expectedDeletedEvents.size()} left")
      }
      logger.info("Successfully awaited all Delete events")
    }

    val expectedReturnedEvents = new util.ArrayList(eventsToReturn.asJava)
    val returnedCompletionAsync = Future {
      while (!expectedReturnedEvents.isEmpty) {
        val event = processing.returnedEvents.takeFirst()
        if (!expectedReturnedEvents.remove(event)) {
          throw new Exception(s"Unexpected event in the Returned queue: $event")
        }

        logger.info(s"Got awaited Return event ${event}, ${expectedReturnedEvents.size()} left")
      }
      logger.info("Successfully awaited all Return events")
    }

    val sendFutures = Random.shuffle(eventsToDelete ++ eventsToReturn)
      .grouped(10)
      .zipWithIndex
      .map { case (group, index) =>
        logger.info(s"Sending events: $group")
        client.sendMessageBatch(
          SendMessageBatchRequest.builder()
            .queueUrl(processing.queueUrl)
            .entries(
              group.map { event =>
                SendMessageBatchRequestEntry.builder()
                  .id(event.id.toString)
                  .messageBody(event.toString)
                  .build()
              }.asJava
            )
            .build()
        ).asScala.map { response =>
          require(response.failed().isEmpty, s"Expected all batch[$index] entries to succeed. Failed: ${response.failed()}")
        }
      }

    sendFutures.foreach { f =>
      Await.result(f, 10.seconds)
    }
    logger.info("All messages have been sent")

    try {
      Try {
        Await.result(deletedCompletionAsync, awaitDuration)
      } match {
        case Success(_) => logger.info("GOT ALL DELETED EVENTS")
        case Failure(ex) => throw new Exception(s"Failed to await all DELETED events. ${expectedDeletedEvents.size()} more is expected", ex)
      }

      Try {
        Await.result(returnedCompletionAsync, awaitDuration)
      } match {
        case Success(_) => logger.info("GOT ALL RETURNED EVENTS")
        case Failure(ex) => throw new Exception(s"Failed to await all RETURN events. ${expectedReturnedEvents.size()} more is expected", ex)
      }

      require(processing.returnedEvents.isEmpty, s"Returned events dequeue contains unexpected elements: ${processing.returnedEvents.asScala}")
      require(processing.deletedEvents.isEmpty, s"Deleted events dequeue contains unexpected elements: ${processing.deletedEvents.asScala}")
    } catch {
      case ex: Exception =>
        checkDLQ(client)
        throw ex
    }

  }

  private def checkDLQ(client: SqsAsyncClient): Unit = {
    val queueUrl = client.getQueueUrl(GetQueueUrlRequest.builder().queueName(s"$QUEUE_NAME-dlq").build()).get().queueUrl()
    val dlqMessages = client.receiveMessage(
      ReceiveMessageRequest.builder()
        .queueUrl(queueUrl)
        .maxNumberOfMessages(10)
        .waitTimeSeconds(5)
        .build()
    ).get().messages()

    if (dlqMessages.isEmpty) {
      logger.info("DLQ is empty")
    } else {
      logger.warn(s"DLQ contains messages: ${dlqMessages}")
    }
  }
}