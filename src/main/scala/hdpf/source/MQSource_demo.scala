package hdpf.source

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object MQSource_demo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // checkpointing is required for exactly-once or at-least-once guarantees
    env.enableCheckpointing(10000)

    val connectionConfig = new RMQConnectionConfig.Builder()
      .setHost("172.31.240.139")
      .setPort(5672)
      .setVirtualHost("aicicd-prod")
      .setUserName("fds-ft")
      .setPassword("Fds-ft@2020")
//      .setConnectionTimeout(10000)
//      .setAutomaticRecovery(true)
//      .setNetworkRecoveryInterval(10000)
    .build

    val stream = env
      .addSource(new RMQSource[String](
        connectionConfig,            // config for the RabbitMQ connection
        "sensor_data1",                // name of the RabbitMQ queue to consume
        false,                        // use correlation ids; can be false if only at-least-once is required
        new SimpleStringSchema))     // deserialization schema to turn messages into Java objects
      .setParallelism(1)               // non-parallel source is only required for exactly-once
    stream.print("hah")
    env.execute()
  }
}
