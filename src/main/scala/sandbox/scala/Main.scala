package sandbox.scala

import org.elasticmq.rest.sqs.SQSRestServerBuilder
import sandbox.scala.Static._
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider
import software.amazon.awssdk.core.client.config.{ClientAsyncConfiguration, ClientOverrideConfiguration, SdkAdvancedAsyncClientOption}
import software.amazon.awssdk.http.nio.netty.{NettyNioAsyncHttpClient, SdkEventLoopGroup}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model._

import java.net.URI
import java.time.Duration
import java.{lang, util}
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CompletableFuture, Executor, Executors, LinkedBlockingDeque, ScheduledExecutorService, ThreadFactory, TimeUnit}
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._
import scala.util.{Failure, Random, Success, Try, Using}

object Main {
  def main(args: Array[String]): Unit = {
    new App().run()
  }
}

class App {
  def run(): Unit = {
    val server = SQSRestServerBuilder.withDynamicPort.withInterface("localhost").start
    try {
      val port = server.waitUntilStarted.localAddress.getPort
      println("Started elasticmq server")
      doTest(s"http://localhost:$port")
    } catch {
      case ex: Throwable =>
        System.err.println(s"Failed to complete the test: ${ex}")
        ex.printStackTrace()
    } finally {
      println("FINALIZING...")
      server.stopAndWait()
      println("FINALIZED")
    }
    println("Exiting...")
  }

  private def doTest(sqsEndpoint: String) {
    Using(initClient(sqsEndpoint)) { client =>
      val createQueueResponse = Await.result(createQueue(client), 10.seconds)
      println("Created the queue")

      Using(new QueueProcessing(sqsEndpoint, createQueueResponse.queueUrl())) { processing =>
        (1 to 100).foreach { i =>
          println(s"TEST ITERATION ${i}")
          sendMessagesAndWait(client, processing)
        }
        println("ALL ITERATIONS COMPLETED SUCCESSFULLY")
      }.get
      println("Stopped queue processing")
    }.get
  }

  private def sendMessagesAndWait(client: SqsAsyncClient, processing: QueueProcessing): Unit = {
    require(processing.returnedEvents.isEmpty, "Queue container unexpected RETURN events")
    require(processing.deletedEvents.isEmpty, "Queue container unexpected DELETE events")

    val eventsToDelete = (1 to 100).map(_ => new Event(EventType.Delete, UUID.randomUUID()))
    val eventsToReturn = (1 to 150).map(_ => new Event(EventType.Return, UUID.randomUUID()))

    val expectedDeletedEvents = new util.ArrayList(eventsToDelete.asJava)
    val deletedCompletionAsync = Future {
      while (!expectedDeletedEvents.isEmpty) {
        val event = processing.deletedEvents.takeFirst()
        if (!expectedDeletedEvents.remove(event)) {
          throw new Exception(s"Unexpected event in the Deleted queue: $event")
        }

        println(s"Got awaited Delete event ${event}, ${expectedDeletedEvents.size()} left")
      }
      println("Successfully awaited all Delete events")
    }

    val expectedReturnedEvents = new util.ArrayList(eventsToReturn.asJava)
    val returnedCompletionAsync = Future {
      while (!expectedReturnedEvents.isEmpty) {
        val event = processing.returnedEvents.takeFirst()
        if (!expectedReturnedEvents.remove(event)) {
          throw new Exception(s"Unexpected event in the Returned queue: $event")
        }

        println(s"Got awaited Return event ${event}, ${expectedReturnedEvents.size()} left")
      }
      println("Successfully awaited all Return events")
    }

    val sendFutures = Random.shuffle(eventsToDelete ++ eventsToReturn)
      .grouped(10)
      .zipWithIndex
      .map { case (group, index) =>
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
          require(response.failed().isEmpty, s"Expected all batch[$index] entries to succeed: ${response.failed()}")
          println(s"Successfully sent a batch ${response.successful()}")
        }
      }

    sendFutures.foreach { f =>
      Await.result(f, 10.seconds)
    }
    println("All messages have been sent")

    try {
      Try {
        Await.result(deletedCompletionAsync, 15.seconds)
      } match {
        case Success(_) => println("GOT ALL DELETED EVENTS")
        case Failure(ex) => throw new Exception(s"Failed to await all DELETED events. ${expectedDeletedEvents.size()} more is expected", ex)
      }

      Try {
        Await.result(returnedCompletionAsync, 15.seconds)
      } match {
        case Success(_) => println("GOT ALL RETURNED EVENTS")
        case Failure(ex) => throw new Exception(s"Failed to await all RETURN events. ${expectedReturnedEvents.size()} more is expected", ex)
      }
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
      println("DLQ is empty")
    } else {
      println(s"DLQ contains messages: ${dlqMessages}")
    }
  }
}

class QueueProcessing(sqsEndpoint: String, val queueUrl: String) extends AutoCloseable{
  val deletedEvents = new LinkedBlockingDeque[Event]()
  val returnedEvents = new LinkedBlockingDeque[Event]()
  private val receivedMessages = new LinkedBlockingDeque[Message]()
  private val messagesToReturn = new LinkedBlockingDeque[Message]()
  private val messagesToDelete = new LinkedBlockingDeque[Message]()

  private val stopped = new AtomicBoolean(false)

  private val client = initClient(sqsEndpoint)

  private val messagePollingExecutor = Executors.newSingleThreadExecutor(new DaemonThreadFactory("message-polling"))
  private val messageProcessingExecutor = Executors.newSingleThreadExecutor(new DaemonThreadFactory("message-processing"))
  private val messageDeletionExecutor = Executors.newSingleThreadExecutor(new DaemonThreadFactory("message-deletion"))
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
    messageDeletionExecutor.execute(() => {
      println("Started message deletion")
      try {
        while (!stopped.get()) {
          val m = messagesToDelete.takeFirst()
          if (stopped.get()) return

          deleteMessages(client, List(m)).get()
          deletedEvents.add(Event.parse(m.body()))
          println(s"Deleted message ${m}")
        }
      } catch {
        case _: InterruptedException => println("Message deletion canceled")
      } finally {
        println("Stopped message deletion")
      }
    })
  }

  private def startMessageReturning(): Unit = {
    messageReturningExecutor.execute(new Runnable {
      override def run(): Unit = {
        println("Started message returning")
        try {
          while (!stopped.get()) {
            val batch = pollBatch(messagesToReturn, 10)
            if (batch.nonEmpty) {
              returnMessages(client, batch).get()
              returnedEvents.addAll(batch.map { it => Event.parse(it.body()) }.asJava)
              println(s"Returned messages batch ${batch}")
            }

            if (batch.size < 10) {
              if (stopped.get()) return
              lang.Thread.sleep(100)
            }
          }
        } catch {
          case _: InterruptedException => println("Message returning canceled")
        } finally {
          println("Stopped message returning")
        }
      }
    })
  }

  private def startMessagePolling(): Unit = {
    messagePollingExecutor.execute(new Runnable {
      override def run(): Unit = {
        println("Started to poll the queue")
        try {
          while (!stopped.get()) {
            pollQueue(client, queueUrl)
          }
        } catch {
          case _: InterruptedException => println("Queue polling canceled")
        } finally {
          println("Stopped queue polling")
        }
      }
    })
  }

  private def startMessageProcessing(): Unit = {
    messageProcessingExecutor.execute(new Runnable {
      override def run(): Unit = {
        println("Started to process the messages")

        try {
          while (!stopped.get()) {
            val message = receivedMessages.takeFirst()

            if (stopped.get()) return

            val event = Event.parse(message.body())
            event match {
              case Event(EventType.Delete, _) =>
                println(s"Got a Delete message: ${message}")
                messagesToDelete.add(message)
              case Event(EventType.Return, _) =>
                println(s"Got a Return message: ${message}")
                messagesToReturn.add(message)
            }
          }
        } catch {
          case _: InterruptedException => println("Message processing canceled")
        } finally {
          println("Stopped to process the messages")
        }
      }
    })
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

    println(s"Got ${response.messages().size()} messages from the queue: ${response.messages()}")

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
        System.err.println(s"Some of the messages failed to delete: ${resp.failed()}")
      } else {
        println(s"Deleted ${resp.successful()}")
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
              .visibilityTimeout(30_000)
              .build()
          ).asJava
        )
        .build()
    ).thenApply(resp => {
      if (resp.hasFailed) {
        System.err.println(s"Some of the messages failed to change the visibility timeout: ${resp.failed()}")
      } else {
        println(s"Returned ${resp.successful()}")
      }
    })
  }
}

object Static {
  final val QUEUE_NAME = "my-queue"

  val sharedExecutor: ScheduledExecutorService = Executors.newScheduledThreadPool(
    Runtime.getRuntime().availableProcessors() - 1,
    new DaemonThreadFactory("shared-threads")
  )
  implicit val sharedExecutionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(sharedExecutor)

  def initClient(sqsEndpoint: String): SqsAsyncClient = SqsAsyncClient.builder()
    .httpClient(
      NettyNioAsyncHttpClient.builder()
        .eventLoopGroup(
          SdkEventLoopGroup.builder()
            .threadFactory(r => Thread(r, "event-loop-thread", isDaemon = true))
            .build()
        )
        .connectionMaxIdleTime(Duration.ofSeconds(5))
        .build()
    )
    .endpointOverride(new URI(sqsEndpoint))
    .region(Region.EU_WEST_1)
    .credentialsProvider(AnonymousCredentialsProvider.create())
    .overrideConfiguration(
      ClientOverrideConfiguration.builder().putHeader("Host", sqsEndpoint.replace("http://", "")).build()
    )
    .asyncConfiguration(
      ClientAsyncConfiguration.builder().advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, DirectExecutor).build()
    )
    .build()

  def createQueue(client: SqsAsyncClient): Future[CreateQueueResponse] =
    client.createQueue(CreateQueueRequest.builder().queueName(s"$QUEUE_NAME-dlq").build())
      .thenCompose(dlqResp => {
        client.getQueueAttributes(
          GetQueueAttributesRequest.builder()
            .queueUrl(dlqResp.queueUrl())
            .attributeNames(QueueAttributeName.QUEUE_ARN)
            .build()
        )
      })
      .thenApply(dlqAttrResp => dlqAttrResp.attributes().get(QueueAttributeName.QUEUE_ARN))
      .thenCompose(dlqArn => {
        client.createQueue(
          CreateQueueRequest.builder()
            .queueName(QUEUE_NAME)
            .attributes(
              Map(QueueAttributeName.REDRIVE_POLICY -> s"""{ "maxReceiveCount": "3", "deadLetterTargetArn": "$dlqArn" }""").asJava
            )
            .build()
        )
      })
      .asScala
}

object DirectExecutor extends Executor {
  override def execute(command: Runnable): Unit = command.run()
}

private object Thread {
  def apply(r: Runnable, name: String, isDaemon: Boolean): Thread = {
    val t = new Thread(r, name)
    t.setDaemon(isDaemon)
    t
  }
}

object EventType extends Enumeration {
  val Delete, Return = Value
}

case class Event(eventType: EventType.Value, id: UUID) {
  override def toString: String = s"$eventType::$id"
}

object Event {
  def parse(value: String): Event = {
    val parts = value.split("::")
    if (parts.size != 2) {
      throw new RuntimeException(s"Unexpected event $value")
    }

    Event(EventType.withName(parts(0)), UUID.fromString(parts(1)))
  }
}

class DaemonThreadFactory(name: String) extends ThreadFactory {
  override def newThread(r: Runnable): Thread = {
    val t = new Thread(r, name)
    t.setDaemon(true)
    t
  }
}