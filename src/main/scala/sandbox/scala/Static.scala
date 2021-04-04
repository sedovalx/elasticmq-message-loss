package sandbox.scala

import com.typesafe.scalalogging.Logger
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model._

import java.time.Instant
import java.util.UUID
import java.util.concurrent._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._

object Static {
  private val logger = Logger("sandbox.Static")

  final val QUEUE_NAME = "my-queue"
  final val DLQ_NAME = s"$QUEUE_NAME-dlq"

  val sharedExecutor: ScheduledExecutorService = Executors.newScheduledThreadPool(
    Runtime.getRuntime().availableProcessors() - 1,
    new DaemonThreadFactory("shared-threads")
  )
  implicit val sharedExecutionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(sharedExecutor)

  def createQueues(client: SqsAsyncClient): Future[CreateQueueResponse] =
    client.createQueue(CreateQueueRequest.builder().queueName(DLQ_NAME).build())
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

  def deleteQueues(client: SqsAsyncClient): CompletableFuture[Unit] =
    client.getQueueUrl(GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build())
    .thenApply(resp => resp.queueUrl())
    .thenCompose(queueUrl => {
      client.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build())
        .thenApply(_ => logger.info(s"Deleted queue $queueUrl"))
    })
    .thenCompose(_ => client.getQueueUrl(GetQueueUrlRequest.builder().queueName(DLQ_NAME).build()))
    .thenApply(resp => resp.queueUrl())
    .thenCompose(queueUrl => {
      client.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build())
        .thenApply(_ => logger.info(s"Deleted queue $queueUrl"))
    })
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

case class Event(eventType: EventType.Value, id: UUID = UUID.randomUUID(), timestamp: Instant = Instant.now()) {
  override def toString: String = s"$eventType::$id::$timestamp"
}

object Event {
  def parse(value: String): Event = {
    val parts = value.split("::")
    if (parts.size != 3) {
      throw new RuntimeException(s"Unexpected event $value")
    }

    Event(EventType.withName(parts(0)), UUID.fromString(parts(1)), Instant.parse(parts(2)))
  }
}

class DaemonThreadFactory(name: String) extends ThreadFactory {
  override def newThread(r: Runnable): Thread = {
    val t = new Thread(r, name)
    t.setDaemon(true)
    t
  }
}
