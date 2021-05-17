package hdpf.operator.window.allWindow

import java.text.SimpleDateFormat
import java.util.{Date, Random}

import hdpf.bean.sink.{StopNumber, TrafficVolume}
import hdpf.bean.source.Participant
import hdpf.utils.Meter.getDistance
import org.apache.flink.streaming.api.scala.function.AllWindowFunction

import scala.collection.mutable.ArrayBuffer
//import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class TrafficVolumeAllWindowApply extends AllWindowFunction[Participant, TrafficVolume, TimeWindow] {


  override def apply(window: TimeWindow, input: Iterable[Participant], out: Collector[TrafficVolume]): Unit = {
    val result = input.groupBy(_.id).size
    val start: Long = window.getStart
    val end: Long = window.getEnd
    val eventStart = new Date(start)
    val eventEnd = new Date(end)
    val eventStartStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(eventStart)
    val eventEndStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(eventEnd)
    val trafficVolume = TrafficVolume(eventStartStr, eventEndStr, result, 1)
    out.collect(trafficVolume)
  }


}
