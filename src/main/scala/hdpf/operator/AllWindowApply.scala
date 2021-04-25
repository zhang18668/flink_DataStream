package hdpf.operator

import java.text.SimpleDateFormat
import java.util.Date

import hdpf.bean.{Participant, Statistics}
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
//import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class AllWindowApply extends AllWindowFunction[Participant, Statistics, TimeWindow] {


  def apply(window: TimeWindow, input: Iterable[Participant], out: Collector[Statistics]): Unit = {
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
    val statistics = Statistics(eventStartStr, eventEndStr, length)
    print("statistics" + statistics)
    out.collect(statistics)
  }
}
