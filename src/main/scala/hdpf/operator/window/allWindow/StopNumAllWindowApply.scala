package hdpf.operator.window.allWindow

import java.text.SimpleDateFormat
import java.util.{Date, Random}

import hdpf.bean.sink.StopNumber
import hdpf.bean.source.Participant
import hdpf.utils.Meter.getDistance
import org.apache.flink.streaming.api.scala.function.AllWindowFunction

import scala.collection.mutable.ArrayBuffer
//import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class StopNumAllWindowApply extends AllWindowFunction[Participant, StopNumber, TimeWindow] {


  def apply(window: TimeWindow, input: Iterable[Participant], out: Collector[StopNumber]): Unit = {
    print("apply执行")
    val set = Set[Participant]()
    //    取出道路上的一个末端点
    var arr = new ArrayBuffer[Int]()
    val lon = 121.612447139
    val lat = 31.2524226077
    for (par <- input) {
      val longitude = par.location.longitude
      val latitude = par.location.latitude
      //获取每个对象车 到 末端点D的距离
      val distance: Int = getDistance(longitude, latitude, lon, lat).toInt / 2
      arr.+=(distance)
    }
//    去重求出input的carid数量
    val inputSize = input.map(x => (x.id, 1)).groupBy(_._1).size
//    取出占比大于百分之20 的数字
    val intToInt: Map[Int, Int] = arr.map((_, 1)).groupBy(_._1).map(x => (x._1, x._2.size)).filter(x => x._2 > arr.length/ (5*inputSize))
    val averStopNum: Int = intToInt.size/inputSize
    val start: Long = window.getStart
    val end: Long = window.getEnd
    val eventStart = new Date(start)
    val eventEnd = new Date(end)
    val eventStartStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(eventStart)
    val eventEndStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(eventEnd)
    //TODO 后续补上
    val random = new Random
    val maxStopNum=((random.nextDouble()+1)*averStopNum).toInt
    val stopNumber = StopNumber(eventStartStr, eventEndStr, maxStopNum, averStopNum, 1)
    out.collect(stopNumber)
  }
}
