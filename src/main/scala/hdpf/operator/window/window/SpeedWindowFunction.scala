package hdpf.operator.window.window

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

import hdpf.bean.sink.StopDelay
import hdpf.bean.source.LaneCar
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}
//窗口函数  按照车的id keyby, 然后时间排序后，相邻的两个速度相减之差乘以10，作为一个新的字段
class SpeedWindowFunction() extends WindowFunction[LaneCar, ListBuffer[LaneCar], Long, TimeWindow] {
  def apply(key: Long, window: TimeWindow, input: Iterable[LaneCar], out: Collector[ListBuffer[LaneCar]]): Unit = {
    var li =ListBuffer[LaneCar]()
    val cars = input.toList.sortBy(_.time).iterator
    var speed_init=0D
    while(cars.hasNext){
      val lanecar = cars.next()
      val speed_now=lanecar.speed
      if(speed_init==0){
        speed_init=speed_now
        lanecar.delta_speed=0
        li +=lanecar
      }else{
        lanecar.delta_speed=(speed_now-speed_init)*10
        println("key:"+key+"speed_now:"+speed_now+"lanecar.delta_speed:"+lanecar.delta_speed)
        li +=lanecar
      }
    }
    out.collect(li)
  }
}
