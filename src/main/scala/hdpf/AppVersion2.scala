package hdpf

import hdpf.bean.{Device, Message, Participant, Payload, Statistics}
import hdpf.operator.{AllWindowApply, IsInPloyin}
import hdpf.sink.MySqlSink
import hdpf.utils.FlinkUtils
import hdpf.watermark.{AssginerWaterMark, AssginerWaterMarkVersion2}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.slf4j.LoggerFactory


object AppVersion2 {

  def main(args: Array[String]): Unit = {
    val hdpfLogger = LoggerFactory.getLogger("hdpf")
    // Flink流式环境的创建
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置env的处理时间为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    // 整合Kafka
    val consumer = FlinkUtils.initKafkaFlink()
    val kafkaDataStream: DataStream[String] = env.addSource(consumer)
    //    kafkaDataStream.print("kafkaDataStream")
    val canalDs = kafkaDataStream.map {
      json => {
        var mes = new Payload(null, null, null)
        try {
          mes = Payload(json)
        }
        catch {
          case e: Exception => hdpfLogger.error("解析错误被舍弃!")
        }
        mes
      }
    }
    //    canalDs.print("canalDs")
    val messagesDS = canalDs.filter(mes => if (mes.version == null || mes.time == null) false else true)
    //    messagesDS.print("ds")
    //添加水印
    //    val strategy: WatermarkStrategy[Message] = WatermarkStrategy.forBoundedOutOfOrderness[Message](Duration.ofSeconds(20)).withTimestampAssigner(new StrategyWaterMark)
    //    val waterDs: DataStream[Message] = canalDs.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[Message](Duration.ofSeconds(20)).withTimestampAssigner(new SerializableTimestampAssigner[Message] {
    //    override def extractTimestamp(element: Message, recordTimestamp: Long): Long = Payload(element.payload).time.toLong
    //  }))
    val waterDs = messagesDS.assignTimestampsAndWatermarks(new AssginerWaterMarkVersion2)
    //链
//    waterDs.print("waterDs")
    val deviceDS: DataStream[Array[Device]] = waterDs.map(_.device_data)
    //    deviceDS.print("deviceDS")
    val devDS: DataStream[Device] = deviceDS.flatMap(x => x)
    val devMapDS: DataStream[Array[Participant]] = devDS.map(_.`object`)
    //    devMapDS.print("devMapDS")
    val parDS: DataStream[Participant] = devMapDS.flatMap(y => y)
    //    parDS.print("parDS")
    val arrFilter: DataStream[Participant] = parDS.filter(new IsInPloyin)
//    arrFilter.print("arr")
    val winDS: DataStream[Statistics] = parDS.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(50), Time.seconds(5))).apply(new AllWindowApply)

    winDS.addSink(new MySqlSink)

    // 执行任务
    env.execute("hdpf_bigdata")
  }
}