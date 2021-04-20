package hdpf.source

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

object DataSource_txt {

  def main(args: Array[String]): Unit = {

    // 1. 流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 2. 读取HDFS中的CSV文件
    val csvDataStream: DataStream[String] = env.readTextFile("E:\\Flink\\flink_DataStream\\src\\main\\resources\\data.txt")
    //  5. 构建`FlinkKafkaProducer010`
    val kafkaCluster = "cdh04:9092,cdh07:9092,cdh08:9092"
    val flinkKafkaProducer011= new FlinkKafkaProducer[String](kafkaCluster,"hdpf", new SimpleStringSchema())
    //  6. 添加sink
    csvDataStream.addSink(flinkKafkaProducer011)
    // 4. 执行任务
    env.execute()
  }

}
