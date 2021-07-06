package hdpf.operator.window.window



import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.api.scala._
import hdpf.bean.sink.StopDelay
import hdpf.bean.source.LaneCar
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class StopDelayWindowFunction() extends WindowFunction[LaneCar, StopDelay, Long, TimeWindow] {
  def apply(key: Long, window: TimeWindow, input: Iterable[LaneCar], out: Collector[StopDelay]): Unit = {
    // 1.根据car的id 分组,映射为id 和timestamp (字符串格式转换long类型)
    //    TODO 可能会有类型转换错误
    val timeTmp: Map[String, Iterable[Long]] = input.groupBy(_.id).map(x => (x._1, x._2.map(_.time)))
    //    根据映射 计算出每个car 在这个窗口出现的时间
    val departureTime: Map[String, Long] = timeTmp.map(x => {
      val departure = x._2.max - x._2.min
      (x._1, departure)
    })
    //    计算出窗口内 汽车的个数
    val departureCount: Int = departureTime.size
    //    累计计算出汽车
    val departureSum: Long = departureTime.values.sum
    val maxdelay: Double = departureTime.values.max.toDouble/1000
    val mindelay: Double = departureTime.values.min.toDouble/1000
    val averdelay = (departureSum / departureCount - 6000).toDouble/1000
    val start: Long = window.getStart
    val end: Long = window.getEnd
    val eventStart = new Date(start)
    val eventEnd = new Date(end)
    val eventStartStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(eventStart)
    val eventEndStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(eventEnd)

    val stopDelay = StopDelay(eventStartStr, eventEndStr, maxdelay, mindelay, averdelay, 1)
    //    print("trafficVolume:" + trafficVolume)
    out.collect(stopDelay)
  }

}
