package hdpf.task

import hdpf.bean.{Device, Participant, Payload, Statistics}
import hdpf.operator.fitter.IsInPloyin
import hdpf.operator.window.AllWindowApply
import hdpf.sink.MySqlSink
import hdpf.utils.{FlinkUtils, GlobalConfigUtil}
import hdpf.watermark.AssginerWaterMarkVersion2
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.slf4j.LoggerFactory
import org.apache.flink.api.scala._
object SpeedFiter {

  def main(args: Array[String]): Unit = {
    val hdpfLogger = LoggerFactory.getLogger("hdpf")
    // Flink流式环境的创建
    val env = FlinkUtils.initFlinkEnv()

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
          case e: Exception => hdpfLogger.error("解析错误被舍弃!"+e.toString)
        }
        mes
      }
    }
    //    canalDs.print("canalDs")
    val messagesDS = canalDs.filter(mes => if (mes.version == null || mes.time == null) false else true)
    messagesDS.print("ds")
    //添加水印
    //        val waterDs: WatermarkStrategy[Payload] = WatermarkStrategy.forBoundedOutOfOrderness[Payload](Duration.ofSeconds(20)).withTimestampAssigner(new StrategyWaterMark)
    //    val waterDs: DataStream[Message] = canalDs.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[Message](Duration.ofSeconds(20)).withTimestampAssigner(new SerializableTimestampAssigner[Message] {
    //    override def extractTimestamp(element: Message, recordTimestamp: Long): Long = Payload(element.payload).time.toLong
    //  }))
    val waterDs: DataStream[Payload] = messagesDS.assignTimestampsAndWatermarks(new AssginerWaterMarkVersion2)
    //链
    //    waterDs.print("waterDs")
    val deviceDS: DataStream[Array[Device]] = waterDs.map(_.device_data)
    //    deviceDS.print("deviceDS")
    val devDS: DataStream[Device] = deviceDS.flatMap(x => x)
    val devMapDS: DataStream[Array[Participant]] = devDS.map(_.`object`)
    //    devMapDS.print("devMapDS")
    val parDS: DataStream[Participant] = devMapDS.flatMap(x => x)
    //    parDS.print("parDS")
    val arrFilter: DataStream[Participant] = parDS.filter(_.speed.toInt!=0)
    arrFilter.print("arr")
//    winDS.addSink(new MySqlSink)
    //    winDS.addSink()

    // 执行任务
    env.execute(GlobalConfigUtil.jobName)
  }
}
