package sandbox.scala

import ch.qos.logback.classic.{Level, LoggerContext}
import com.typesafe.scalalogging.Logger
import org.elasticmq.rest.sqs.SQSRestServerBuilder
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.{AwsCredentialsProvider, ProfileCredentialsProvider}
import software.amazon.awssdk.regions.Region

import scala.io.StdIn.readLine

object Main {
  private val logger = Logger("sandbox.Main")

  def main(args: Array[String]): Unit = {
    val cliArgs = new CliArgs(args)

    if (cliArgs.debug()) {
      setupDebugLogging()
    }

    val (clientMode, closeable) = cliArgs.mode() match {
      case CliClientMode.elasticmq => elasticMqMode()
      case CliClientMode.sqs =>
        val region = cliArgs.awsRegion()
        val awsProfile = cliArgs.awsProfile.toOption match {
          case Some(value) => value
          case None =>
            print("Enter AWS profile name: ")
            readLine()
        }
        sqsMode(region, ProfileCredentialsProvider.builder().profileName(awsProfile).build())
    }

    try {
      new Test(clientMode, cliArgs.iterations(), cliArgs.eventsToDelete(), cliArgs.eventsToReturn(), cliArgs.awaitDuration())
    } finally {
      closeable.close()
    }
  }

  private def setupDebugLogging(): Unit = {
    val debugNames = List("org.elasticmq", "sandbox")
    val loggerContext = LoggerFactory.getILoggerFactory().asInstanceOf[LoggerContext]
    debugNames.foreach { name =>
      loggerContext.getLogger(name).setLevel(Level.DEBUG)
    }
    logger.debug(s"Enabled DEBUG level: $debugNames")
  }

  private def elasticMqMode(): (ClientMode, AutoCloseable) = {
    val server = SQSRestServerBuilder.withDynamicPort.withInterface("localhost").start
    val port = try {
      server.waitUntilStarted.localAddress.getPort
    } catch {
      case ex: Exception =>
        server.stopAndWait()
        throw ex
    }
    logger.info("Started elasticmq server")

    (new ElasticMqClientMode(port), () => server.stopAndWait())
  }

  private def sqsMode(region: Region, credentialsProvider: AwsCredentialsProvider): (ClientMode, AutoCloseable) = {
    (new SqsClientMode(region, credentialsProvider), () => {})
  }
}




