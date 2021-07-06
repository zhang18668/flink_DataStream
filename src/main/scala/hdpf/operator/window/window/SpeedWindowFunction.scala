package hdpf.operator.window.window

import java.text.SimpleDateFormat
import java.util.Date

import hdpf.bean.sink.StopDelay
import hdpf.bean.source.LaneCar
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}
//窗口函数  按照车的id keyby, 然后时间排序后，相邻的两个速度相减之差乘以10，作为一个新的字段
class SpeedWindowFunction() extends WindowFunction[LaneCar, LaneCar, Long, TimeWindow] {
  def apply(key: Long, window: TimeWindow, input: Iterable[LaneCar], out: Collector[LaneCar]): Unit = {
    val cars = input.toList.sortBy(_.time).iterator
    var speed_init=0D
    while(cars.hasNext){
      val speed_now = cars.next().speed
      if(speed_init==0){
        speed_init=speed_now
        cars.next().delta_speed=0
      }else{
        cars.next().delta_speed=(speed_now-speed_init)*10
      }
      out.collect(cars.next())
    }
  }
}
