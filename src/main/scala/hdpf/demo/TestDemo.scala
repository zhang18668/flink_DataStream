package hdpf.demo

import hdpf.bean.{Device, Message, Participant, Payload}
import hdpf.operator.{AllWindowApply, IsInPloyin}
import hdpf.utils.FlinkUtils
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.scala.{AllWindowedStream, DataStream}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{SlidingProcessingTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object TestDemo {

  def main(args: Array[String]): Unit = {
    // Flink流式环境的创建
    val env = FlinkUtils.initFlinkEnv()

    // 整合Kafka
    val consumer = FlinkUtils.initKafkaFlink()

    // 测试打印
    val kafkaDataStream: DataStream[String] = env.addSource(consumer)
    val canalDs = kafkaDataStream.map(json => Message(json))
    canalDs.print()

    val waterDs: DataStream[Message] = canalDs.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Message] {

      // 当前的时间戳
      var currentTimestamp = 0L

      // 延迟的时间
      val delayTime = 2000l

      // 返回水印时间
      override def getCurrentWatermark: Watermark = {
        new Watermark(currentTimestamp - delayTime)
      }

      // 比较当前元素的时间和上一个元素的时间,取最大值,防止时光倒流
      override def extractTimestamp(element: Message, previousElementTimestamp: Long): Long = {
        var time = Payload(element.payload).time.toLong
        currentTimestamp = Math.max(time, previousElementTimestamp)
        currentTimestamp
      }
    })

    val payloadDS: DataStream[Payload] = waterDs.map(message => Payload(message.payload))
    val deviceDS: DataStream[Array[Device]] = payloadDS.map(_.device_data)
    val devDS: DataStream[Device] = deviceDS.flatMap(x => x)
    val devMapDS: DataStream[Array[Participant]] = devDS.map(_.`object`)
    val parDS: DataStream[Participant] = devMapDS.flatMap(y => y)
    val arrFilter: DataStream[Participant] = parDS.filter(new IsInPloyin)
    val winDS: DataStream[Int] = arrFilter.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(60),Time.seconds(10))).apply(new AllWindowApply)

//    arrFilter.timeWindowAll()
      //    ds.timeWindowAll(Time.seconds(4),Time.seconds(8))

      // 执行任务
      env.execute("sync-db")
  }
}
