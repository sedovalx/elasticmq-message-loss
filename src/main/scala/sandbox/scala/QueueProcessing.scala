package sandbox.scala

import com.typesafe.scalalogging.Logger
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{ChangeMessageVisibilityBatchRequest, ChangeMessageVisibilityBatchRequestEntry, DeleteMessageBatchRequest, DeleteMessageBatchRequestEntry, Message, ReceiveMessageRequest}

import java.{lang, util}
import java.util.concurrent.{CompletableFuture, Executors, LinkedBlockingDeque, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

class QueueProcessing(clientMode: ClientMode, val queueUrl: String) extends AutoCloseable{
  private val logger = Logger[QueueProcessing]

  val deletedEvents = new LinkedBlockingDeque[Event]()
  val returnedEvents = new LinkedBlockingDeque[Event]()
  private val receivedMessages = new LinkedBlockingDeque[Message]()
  private val messagesToReturn = new LinkedBlockingDeque[Message]()
  private val messagesToDelete = new LinkedBlockingDeque[Message]()

  private val stopped = new AtomicBoolean(false)

  private val client = clientMode.initClient()

  private val messagePollingExecutor = Executors.newSingleThreadExecutor(new DaemonThreadFactory("message-polling"))
  private val messageProcessingExecutor = Executors.newCachedThreadPool(new DaemonThreadFactory("message-processing"))
  private val messageDeletionExecutor = Executors.newCachedThreadPool(new DaemonThreadFactory("message-deletion"))
  private val messageReturningExecutor = Executors.newSingleThreadExecutor(new DaemonThreadFactory("message-returning"))

  {
    startMessageDeletion()
    startMessageReturning()
    startMessageProcessing()
    startMessagePolling()
  }

  def close(): Unit = {
    stopped.set(true)
    messagePollingExecutor.shutdownNow()
    messageProcessingExecutor.shutdownNow()
    messageDeletionExecutor.shutdownNow()
    messageReturningExecutor.shutdownNow()
    client.close()
  }

  private def startMessageDeletion(): Unit = {
    val processing = new Runnable {
      override def run(): Unit = {
        logger.info("Started message deletion")
        try {
          while (!stopped.get()) {
            val m = messagesToDelete.takeFirst()
            if (stopped.get()) return

            deleteMessages(client, List(m)).get()
            deletedEvents.add(Event.parse(m.body()))
            logger.info(s"Deleted message ${m}")
          }
        } catch {
          case _: InterruptedException => logger.warn("Message deletion canceled")
          case ex => logger.error("Message deletion failed", ex)
        } finally {
          logger.info("Stopped message deletion")
        }
      }
    }

    (1 to 1).foreach(_ => messageDeletionExecutor.execute(processing))
  }

  private def startMessageReturning(): Unit = {
    messageReturningExecutor.execute(new Runnable {
      override def run(): Unit = {
        logger.info("Started message returning")
        try {
          while (!stopped.get()) {
            val batch = pollBatch(messagesToReturn, 10)
            if (batch.nonEmpty) {
              returnMessages(client, batch).get()
              returnedEvents.addAll(batch.map { it => Event.parse(it.body()) }.asJava)
              logger.info(s"Returned messages: ${batch}")
            }

            if (batch.size < 10) {
              if (stopped.get()) return
              lang.Thread.sleep(100)
            }
          }
        } catch {
          case _: InterruptedException => logger.warn("Message returning canceled")
          case ex => logger.error("Message returning failed", ex)
        } finally {
          logger.info("Stopped message returning")
        }
      }
    })
  }

  private def startMessagePolling(): Unit = {
    messagePollingExecutor.execute(new Runnable {
      override def run(): Unit = {
        logger.info("Started to poll the queue")
        try {
          while (!stopped.get()) {
            pollQueue(client, queueUrl)
          }
        } catch {
          case _: InterruptedException => logger.warn("Queue polling canceled")
          case ex => logger.error("Message polling failed", ex)
        } finally {
          logger.info("Stopped queue polling")
        }
      }
    })
  }

  private def startMessageProcessing(): Unit = {
    val processing = new Runnable {
      override def run(): Unit = {
        logger.info("Started to process the messages")

        try {
          while (!stopped.get()) {
            val message = receivedMessages.takeFirst()

            if (stopped.get()) return

            val event = Event.parse(message.body())
            event match {
              case Event(EventType.Delete, _, _) =>
                logger.debug(s"Got a Delete message: ${message}")
                messagesToDelete.add(message)
              case Event(EventType.Return, _, _) =>
                logger.debug(s"Got a Return message: ${message}")
                messagesToReturn.add(message)
            }
          }
        } catch {
          case _: InterruptedException => logger.warn("Message processing canceled")
          case ex => logger.error("Message processing failed", ex)
        } finally {
          logger.info("Stopped to process the messages")
        }
      }
    }

    (1 to 3).foreach(_ => messageProcessingExecutor.execute(processing))
  }

  private def pollBatch[E](queue: LinkedBlockingDeque[E], maxBatchSize: Int): List[E] = {
    val batch = new util.ArrayList[E]()
    while (batch.size() < maxBatchSize) {
      val next = queue.poll()
      if (next != null) {
        batch.add(next)
      } else {
        return batch.asScala.toList
      }
    }

    batch.asScala.toList
  }

  private def pollQueue(client: SqsAsyncClient, queueUrl: String): Unit = {
    if (stopped.get()) return

    val response = client.receiveMessage(
      ReceiveMessageRequest.builder()
        .queueUrl(queueUrl)
        .maxNumberOfMessages(10)
        .waitTimeSeconds(5)
        .build()
    ).get(10, TimeUnit.SECONDS)

    logger.debug(s"Got ${response.messages().size()} events from the queue: ${response.messages().map(it => Event.parse(it.body()))}")

    response.messages().forEach { it =>
      receivedMessages.add(it)
    }
  }

  private def deleteMessages(client: SqsAsyncClient, messages: List[Message]): CompletableFuture[Unit] = {
    client.deleteMessageBatch(
      DeleteMessageBatchRequest.builder()
        .queueUrl(queueUrl)
        .entries(messages.map(it => DeleteMessageBatchRequestEntry.builder().id(it.messageId()).receiptHandle(it.receiptHandle()).build()).asJava)
        .build()
    ).thenApply(resp => {
      if (resp.hasFailed) {
        throw new Exception(s"Some of the messages failed to delete: ${resp.failed()}")
      } else {
        logger.debug(s"Deleted ${resp.successful()}")
      }
    })
  }

  private def returnMessages(client: SqsAsyncClient, messages: List[Message]): CompletableFuture[Unit] = {
    client.changeMessageVisibilityBatch(
      ChangeMessageVisibilityBatchRequest.builder()
        .queueUrl(queueUrl)
        .entries(
          messages.map(it =>
            ChangeMessageVisibilityBatchRequestEntry.builder()
              .id(it.messageId())
              .receiptHandle(it.receiptHandle())
              .visibilityTimeout(60.minutes.toSeconds.toInt)
              .build()
          ).asJava
        )
        .build()
    ).thenApply(resp => {
      if (resp.hasFailed) {
        throw new Exception(s"Some of the messages failed to change the visibility timeout: ${resp.failed()}")
      } else {
        logger.debug(s"Returned ${resp.successful()}")
      }
    })
  }
}
