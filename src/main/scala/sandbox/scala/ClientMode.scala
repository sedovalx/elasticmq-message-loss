package sandbox.scala

import software.amazon.awssdk.auth.credentials.{AnonymousCredentialsProvider, AwsCredentialsProvider}
import software.amazon.awssdk.core.client.config.{ClientAsyncConfiguration, ClientOverrideConfiguration, SdkAdvancedAsyncClientOption}
import software.amazon.awssdk.http.nio.netty.{NettyNioAsyncHttpClient, SdkEventLoopGroup}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsAsyncClient

import java.net.URI
import java.time.Duration

trait ClientMode {
  def initClient(): SqsAsyncClient
}

class SqsClientMode(region: Region, credentialsProvider: AwsCredentialsProvider) extends ClientMode {
  override def initClient(): SqsAsyncClient = {
    SqsAsyncClient.builder()
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
      .region(region)
      .credentialsProvider(credentialsProvider)
      .asyncConfiguration(
        ClientAsyncConfiguration.builder().advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, DirectExecutor).build()
      )
      .build()
  }
}

class ElasticMqClientMode(port: Int) extends ClientMode {
  override def initClient() = {
    val sqsEndpoint = s"http://localhost:$port"
    SqsAsyncClient.builder()
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
  }
}