package hdpf.task

import hdpf.utils.FlinkUtils
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig
import org.apache.flink.streaming.util.serialization.SimpleStringSchema


object App {

  def main(args: Array[String]): Unit = {
//    val hdpfLogger = LoggerFactory.getLogger("hdpf")
    //TODO 1. Flink流式环境的创建
    val env = FlinkUtils.initFlinkEnv()

    // TODO 2. 整合MQ
    val connectionConfig = new RMQConnectionConfig.Builder()
      .setHost("172.31.240.139")
      .setPort(5672)
      .setVirtualHost("ord-ft")
      .setUserName("fds-ft")
      .setPassword("Fds-ft@2020")
      //      .setConnectionTimeout(10000)
      //      .setAutomaticRecovery(true)
      //      .setNetworkRecoveryInterval(10000)
      .build

    val stream = env
      .addSource(new RMQSource[String](
        connectionConfig,            // config for the RabbitMQ connection
        "ord-obu-data-temp-zzh",                // name of the RabbitMQ queue to consume
        false,                        // use correlation ids; can be false if only at-least-once is required
        new SimpleStringSchema))     // deserialization schema to turn messages into Java objects
      .setParallelism(1)               // non-parallel source is only required for exactly-once
//TODO 3.transform
    stream.print()
//TODO 4.sink

//    winDS.addSink(new MySqlSink)

    // 执行任务
    env.execute()
  }
}
