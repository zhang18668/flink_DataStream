package hdpf

import hdpf.bean.{Device, Message, Participant, Payload}
import hdpf.operator.{AllWindowApply, IsInPloyin}
import hdpf.utils.FlinkUtils
import hdpf.watermark.{AssginerWaterMark, StrategyWaterMark}
import javafx.util.Duration
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{SlidingProcessingTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import java.time.Duration


object App {

  def main(args: Array[String]): Unit = {
    // Flink流式环境的创建
    val env = FlinkUtils.initFlinkEnv()

    // 整合Kafk

    val consumer = FlinkUtils.initKafkaFlink()
    val kafkaDataStream: DataStream[String] = env.addSource(consumer)
    val canalDs = kafkaDataStream.map(json => Message(json))
    //添加水印
    val waterDs: DataStream[Message] = canalDs.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[Message](java.time.Duration.ofSeconds(20)).withTimestampAssigner(new StrategyWaterMark))
    //链
    val payloadDS: DataStream[Payload] = waterDs.map(message => Payload(message.payload))
    val deviceDS: DataStream[Array[Device]] = payloadDS.map(_.device_data)
    val devDS: DataStream[Device] = deviceDS.flatMap(x => x)
    val devMapDS: DataStream[Array[Participant]] = devDS.map(_.`object`)
    val parDS: DataStream[Participant] = devMapDS.flatMap(y => y)
    val arrFilter: DataStream[Participant] = parDS.filter(new IsInPloyin)
    val winDS: DataStream[Int] = arrFilter.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(60),Time.seconds(10))).apply(new AllWindowApply)



    // 执行任务
    env.execute("sync-db")
  }
}
