package hdpf.operator.window

import java.text.SimpleDateFormat
import java.util.Date

import hdpf.bean.sink.TrafficVolume
import hdpf.bean.source.Participant
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
//import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class TrafficVolumeAllWindowApply extends AllWindowFunction[Participant, TrafficVolume, TimeWindow] {


  def apply(window: TimeWindow, input: Iterable[Participant], out: Collector[TrafficVolume]): Unit = {
    print("apply执行")
    val set = Set[Participant]()
    val strings = input.map(x => x.id)
    println(strings)
    println(strings.toArray.length)
    val length: Int = strings.toSet.toArray.length
    println(length)
    val start: Long = window.getStart
    val end: Long = window.getEnd
    val eventStart = new Date(start)
    val eventEnd = new Date(end)
    val eventStartStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(eventStart)
    val eventEndStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(eventEnd)
    val trafficVolume = TrafficVolume(eventStartStr, eventEndStr, length,1)
//    print("trafficVolume:" + trafficVolume)
    out.collect(trafficVolume)
  }
}
