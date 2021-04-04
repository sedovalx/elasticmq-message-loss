package sandbox.scala

import org.rogach.scallop._
import sandbox.scala.CliArgs.regionConverter
import software.amazon.awssdk.regions.Region

import scala.concurrent.duration.{Duration, DurationInt}

class CliArgs(args: Seq[String]) extends ScallopConf(args) {
  val mode = opt[CliClientMode.Value](
    default = Some(CliClientMode.elasticmq),
    descr = "Backend mode. Options: elasticmq or sqs. Default: elasticmq",
    argName = "String"
  )(CliClientMode.converter)
  val awsRegion = opt[Region](
    default = Some(Region.EU_WEST_1),
    noshort = true,
    descr = "AWS region name. Required only for the SQS mode. Default: EU_WEST_1",
    argName = "String"
  )(regionConverter)
  val awsProfile = opt[String](
    noshort = true,
    descr = "AWS profile name to locate credentials info. Required only for the SQS mode",
    argName = "String"
  )
  val iterations = opt[Int](
    default = Some(100),
    descr = "Number of test iterations. Default: 100",
    argName = "Int"
  )
  val eventsToDelete = opt[Int](
    name = "de",
    default = Some(100),
    descr = "Number of sent messages that should be deleted from the queue during the polling. Default: 100",
    argName = "Int",
    noshort = true
  )
  val eventsToReturn = opt[Int](
    name = "re",
    default = Some(150),
    descr = "Number of sent messages that should be returned to the queue after a visibility timeout during the polling. Default: 150",
    argName = "Int",
    noshort = true
  )
  val awaitDuration = opt[Duration](
    default = Some(15.seconds),
    descr = "How long the test should wait until all expected messages are processed. Default: 15 seconds",
    argName = "Duration"
  )
  val debug = opt[Boolean](default = Some(false), descr = "Enables debug logging", argName = "Boolean")

  verify()
}

object CliArgs {
  private val regionConverter = singleArgConverter { s => Region.of(s) }
}

object CliClientMode extends Enumeration {
  val sqs, elasticmq = Value

  val converter: ValueConverter[CliClientMode.Value] = singleArgConverter { s => CliClientMode.withName(s) }
}



