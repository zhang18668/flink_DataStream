package hdpf

import hdpf.bean.{Device, Message, Participant, Payload}
import hdpf.utils.{FlinkUtils, GlobalConfigUtil}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import hdpf.sink.MySqlSink
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

import scala.util.parsing.json.JSON

object App {

  def main(args: Array[String]): Unit = {
    // Flink流式环境的创建
    val env = FlinkUtils.initFlinkEnv()

    // 整合Kafka
    val consumer = FlinkUtils.initKafkaFlink()

    // 测试打印
    val kafkaDataStream: DataStream[String] = env.addSource(consumer)
    val canalDs = kafkaDataStream.map { json => Message(json) }
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
    //算法
    case class Point(var x: Double, var y: Double) {
      def getPoint = Point.apply(x, y)
    }
    /**
      * 判断点是否在多边形内部
      */
    def isInPloyin(p: Point, pts: List[Point]) = {
      var intersectionp = 0
      for (i <- pts.indices) {

        val p1 = pts(i)
        val p2 = pts((i + 1) % pts.size)

        if (p.y >= Array(p1.y, p2.y).min && p.y < Array(p1.y, p2.y).max)
          if (((p.y - p1.y) * (p2.x - p1.x) / (p2.y - p1.y) + p1.x) < p.x)
            intersectionp += 1
      }
      intersectionp % 2
    }

    var ds = waterDs.map {
      tex =>
        val list = List(Point(114.0717, 30.44909), Point(114.0717, 30.44910), Point(114.0722, 30.44909), Point(114.0722, 30.44910))

        var message: Message = tex
        var payload: Payload = Payload(message.payload)
        var devices: Array[Device] = payload.device_data
        var set = scala.collection.mutable.Set[String]()

        for (dev <- devices) {
          var objects: Array[Participant] = dev.`object`
          for (obj <- objects) {
            var p = Point(obj.location.longitude, obj.location.latitude)
            val i = isInPloyin(p, list)
            println("longitude:" + obj.location.longitude + "latitude:" + obj.location.latitude + "在不在:====" + i)
            println(i)
            if (i == 1) set += obj.id
            //            obj
          }
          set.toArray
        }
    }

//    ds.timeWindowAll(Time.seconds(4),Time.seconds(8))

    // 执行任务
    env.execute("sync-db")
  }
}
