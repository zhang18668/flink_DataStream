package hdpf.task

import hdpf.utils.{FlinkUtils, GlobalConfigUtil}
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
    val connectionConfig = FlinkUtils.initMQFlink()

    val stream = env.addSource(new RMQSource[String](connectionConfig, GlobalConfigUtil.rabbitmqQueueName, false, new SimpleStringSchema)).setParallelism(1)
//TODO 3.transform
    stream.print()
//TODO 4.sink

//    winDS.addSink(new MySqlSink)

    // 执行任务
    env.execute()
  }
}
