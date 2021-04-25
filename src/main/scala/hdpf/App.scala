package hdpf

import hdpf.bean.{Device, Message, Participant, Payload}
import hdpf.operator.{AllWindowApply, IsInPloyin}
import hdpf.utils.{FlinkUtils, GlobalConfigUtil}
import hdpf.watermark.{AssginerWaterMark, StrategyWaterMark}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{SlidingProcessingTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import java.time.Duration
import java.util.Properties

import hdpf.sink.MySqlSink
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.slf4j.Logger
import org.slf4j.LoggerFactory


object App {

  def main(args: Array[String]): Unit = {
    val hdpfLogger = LoggerFactory.getLogger("hdpf")
    // Flink流式环境的创建
    val env = FlinkUtils.initFlinkEnv()


    // 整合Kafka
    val consumer = FlinkUtils.initKafkaFlink()
    val kafkaDataStream: DataStream[String] = env.addSource(consumer)
    kafkaDataStream.print("kafkaDataStream")
    val canalDs = kafkaDataStream.map {
      json => {
        var mes = new Message(null, null, null)
        try {
          mes = Message(json)
        }
        catch {
          case e: Exception => hdpfLogger.error("解析错误被舍弃!")
        }
        mes
      }
    }
    val messagesDS: DataStream[Message] = canalDs.filter(mes => if (mes.vhost == null || mes.name == null) false else true)
    messagesDS.print("ds")
    //添加水印
    //    val strategy: WatermarkStrategy[Message] = WatermarkStrategy.forBoundedOutOfOrderness[Message](Duration.ofSeconds(20)).withTimestampAssigner(new StrategyWaterMark)
    //    val waterDs: DataStream[Message] = canalDs.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[Message](Duration.ofSeconds(20)).withTimestampAssigner(new SerializableTimestampAssigner[Message] {
    //    override def extractTimestamp(element: Message, recordTimestamp: Long): Long = Payload(element.payload).time.toLong
    //  }))
    val waterDs: DataStream[Message] = messagesDS.assignTimestampsAndWatermarks(new AssginerWaterMark)
    //链
    waterDs.print("waterDs")
    val payloadDS: DataStream[Payload] = waterDs.map(message => Payload(message.payload))
    //    payloadDS.print("payloadDS")
    val deviceDS: DataStream[Array[Device]] = payloadDS.map(_.device_data)
    //    deviceDS.print("deviceDS")
    val devDS: DataStream[Device] = deviceDS.flatMap(x => x)
    val devMapDS: DataStream[Array[Participant]] = devDS.map(_.`object`)
    //    devMapDS.print("devMapDS")
    val parDS: DataStream[Participant] = devMapDS.flatMap(y => y)
    //    parDS.print("parDS")
    val arrFilter: DataStream[Participant] = parDS.filter(new IsInPloyin)
    arrFilter.print("arr")
    val winDS = parDS.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))).apply(new AllWindowApply)
//    winDS.addSink(new MySqlSink)
    //    winDS.print("哈哈")

    // 执行任务
    env.execute("hdpf_bigdata")
  }
}
