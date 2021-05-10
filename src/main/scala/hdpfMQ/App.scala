package hdpfMQ

import hdpf.utils.FlinkUtils
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._

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
      .build

    val stream = env
      .addSource(new RMQSource[String](
        connectionConfig,
        "ord-roadside-fusion-result-zzh",
        false,
        new SimpleStringSchema))
      .setParallelism(1)

//TODO 3.transform 排队长度 包括绿灯启亮时刻排队长度、红灯启亮时刻排队长度、周期平均排队长度等

    stream.print("hah")
//TODO 4.sink

//    winDS.addSink(new MySqlSink)

    // 执行任务
    env.execute()
  }
}
